#include "elog/client/consumer.h"

#include <algorithm>
#include <csignal>
#include <cstdlib>
#include <utility>

#include "elog/client/fetcher.h"
#include "elog/client/membership_client.h"
#include "elog/client/offset_committer.h"
#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/error_codes.h"

namespace elog {

Consumer::Consumer(ConsumerConfig cfg, Metrics& metrics)
    : cfg_(std::move(cfg)),
      metrics_(metrics),
      metadata_(cfg_.coordinator),
      inflight_(cfg_.inflightMax),
      fetcher_(std::make_unique<Fetcher>(cfg_, metadata_, state_, inflight_, metrics_)) {}

Consumer::~Consumer() {
    close();
    fetcher_->stop();
}

Status Consumer::run(const std::function<void(const ConsumedRecord&)>& process) {
    if (auto started = cfg_.groupId.empty() ? startManual() : startGroup(); !started.ok()) {
        return started;
    }
    fetcher_->start();
    while (!closed_) {
        if (membership_ && (membership_->fenced || membership_->unknownMember)) {
            onFenced();
            continue;
        }
        if (membership_ && membership_->rebalanceRequested) {
            onRebalance();
            continue;
        }
        auto record = inflight_.pop(std::chrono::milliseconds(100));
        if (record && state_.owns(record->tp, record->assignedVersion)) {
            process(*record);
            state_.markProcessed(record->tp, record->offset + 1);
            const std::string labels =
                "topic=" + record->tp.topic + ";partition=" + std::to_string(record->tp.partition);
            metrics_.counter("consume_records_total", labels).add();
            metrics_.histogram("e2e_latency_ms")
                .record(static_cast<double>(time::wallMs() - record->timestampMs));
            if (committer_) {
                committer_->onProcessed();
            }
            if (processedSinceCommit_) {
                ++*processedSinceCommit_;
                maybeCrash();  // §14: after processing, before the commit below
            }
        }
        if (committer_ && committer_->due()) {
            doCommit();
        }
    }
    fetcher_->stop();
    if (membership_) {  // final commit, then leave so the group rebalances at once
        doCommit();
        membership_->leave();
        membership_->stop();
    }
    return okStatus();
}

Status Consumer::startManual() {
    auto partitions = partitionsToRead();
    if (!partitions.ok()) {
        return {partitions.error(), partitions.message()};
    }
    logging::get("consumer")
        ->info("consuming mode=manual partitions={}", partitions.value().size());
    std::vector<PartitionEpoch> assignment;
    for (const TopicPartition& tp : partitions.value()) {
        assignment.push_back({tp.topic, tp.partition, 0});
    }
    state_.apply(0, assignment);  // a fixed assignment, no group
    return okStatus();
}

Status Consumer::startGroup() {
    if (auto status = metadata_.refresh(cfg_.topics); !status.ok()) {
        return status;
    }
    membership_ = std::make_unique<MembershipClient>(cfg_);
    committer_ = std::make_unique<OffsetCommitter>(
        cfg_.coordinator, cfg_.groupId, cfg_.commitIntervalRecords, cfg_.commitIntervalTime);
    membership_->startHeartbeats();
    return joinAndApply();
}

// LLD §10.6: join, apply the assignment, drop buffered records of revoked partitions, and
// start every added partition at its committed offset (or per auto.offset.reset).
Status Consumer::joinAndApply() {
    auto joined = membership_->join(state_.ownedWithEpochs());
    if (!joined.ok()) {
        return {joined.error(), joined.message()};
    }
    eagerAssignor_ = joined.value().assignor != "sticky";  // LLD §9: only sticky is not eager
    const AssignmentDelta delta =
        state_.apply(joined.value().generation, joined.value().assignment);
    dropBuffered(delta.revoked);
    startAddedPartitions(delta.added);
    logging::get("consumer")
        ->info("assignment_received generation={} added={} revoked={} retained={}",
               joined.value().generation, delta.added.size(), delta.revoked.size(),
               delta.retained.size());
    return okStatus();
}

void Consumer::startAddedPartitions(const std::vector<TopicPartition>& added) {
    if (added.empty()) {
        return;
    }
    auto committed = committer_->fetchCommitted(added);
    if (!committed.ok()) {  // positions stay unset: the fetcher applies auto.offset.reset
        logging::get("consumer")->warn("offset_fetch_failed error=\"{}\"", committed.message());
        return;
    }
    for (const OffsetFetchEntry& e : committed.value()) {
        if (e.error == ErrorCode::NONE && e.offset >= 0) {
            state_.startAt({e.topic, e.partition}, static_cast<Offset>(e.offset), true);
        }
    }
}

// Consuming → Revoking → Joining (LLD §10.6): commit first; an eager assignor then gives
// up everything. Under sticky, owned partitions keep fetching through the join, and those
// it retains continue where they were, with no pause and no offset fetch.
void Consumer::onRebalance() {
    membership_->rebalanceRequested = false;  // a request during the join triggers another
    doCommit();
    if (eagerAssignor_) {
        state_.revokeAll();
        dropAllBuffered();
    }
    if (auto status = joinAndApply(); !status.ok()) {
        logging::get("consumer")->error("rejoin_failed error=\"{}\"", status.message());
        closed_ = true;
    }
}

// FR-OFF-11: fenced (a commit got STALE_EPOCH) or evicted (e.g. paused past the session
// timeout): nothing it holds is its own any more, so drop everything, buffered records
// included, and join again as a new member.
void Consumer::onFenced() {
    logging::get("consumer")
        ->warn("fenced reason={} generation={}", membership_->fenced ? "stale_epoch" : "evicted",
               state_.generation());
    membership_->fenced = false;
    membership_->unknownMember = false;
    membership_->rebalanceRequested = false;
    state_.revokeAll();
    dropAllBuffered();
    membership_->resetMemberId();
    if (auto status = joinAndApply(); !status.ok()) {
        logging::get("consumer")->error("rejoin_failed error=\"{}\"", status.message());
        closed_ = true;
    }
}

void Consumer::doCommit() {
    if (cfg_.beforeCommit) {
        cfg_.beforeCommit();
    }
    const bool anythingToCommit = !state_.pendingCommits().empty();
    auto errors = committer_->commit(membership_->memberId(), state_);
    if (!errors.ok()) {
        logging::get("consumer")->warn("commit_failed error=\"{}\"", errors.message());
        return;
    }
    if (anythingToCommit && errors.value().empty()) {  // R-24: arms or restarts the count
        processedSinceCommit_ = 0;
        maybeCrash();
    }
    for (const PartitionError& e : errors.value()) {
        logging::get("consumer")
            ->warn("commit_rejected topic={} partition={} error={}", e.topic, e.partition,
                   errorName(e.error));
        if (e.error == ErrorCode::REBALANCE_IN_PROGRESS ||
            e.error == ErrorCode::ILLEGAL_GENERATION) {
            membership_->rebalanceRequested = true;
        } else if (e.error == ErrorCode::UNKNOWN_MEMBER) {
            membership_->unknownMember = true;
        } else if (e.error == ErrorCode::STALE_EPOCH) {
            membership_->fenced = true;
        }
    }
}

// LLD §14, R-24: a deterministic crash between processing and committing, so exactly k
// records are delivered again after the restart (FR-DEL-04).
void Consumer::maybeCrash() {
    if (!cfg_.crashAfterProcess || !processedSinceCommit_ ||
        *processedSinceCommit_ < cfg_.crashAfterProcess->k) {
        return;
    }
    if (cfg_.beforeCrash) {
        cfg_.beforeCrash();
    }
    // Every log line is flushed as it is written, so this one is on disk before the kill.
    logging::get("consumer")
        ->warn("crash_hook_fired processed_since_commit={}", *processedSinceCommit_);
    std::raise(SIGKILL);
    // As PID 1 of a container a process cannot signal itself; never carry on past the crash.
    std::_Exit(128 + SIGKILL);
}

void Consumer::dropBuffered(const std::vector<TopicPartition>& partitions) {
    inflight_.removeIf([&](const ConsumedRecord& r) {
        return std::find(partitions.begin(), partitions.end(), r.tp) != partitions.end();
    });
}

void Consumer::dropAllBuffered() {
    inflight_.removeIf([](const ConsumedRecord&) { return true; });
}

void Consumer::close() {
    closed_ = true;
}

// R-16: the configured partitions, or every partition of the configured topics. Either
// way their leaders are looked up now, before the first fetch.
Result<std::vector<TopicPartition>> Consumer::partitionsToRead() {
    std::vector<std::string> topics = cfg_.topics;
    for (const TopicPartition& tp : cfg_.assignment) {
        topics.push_back(tp.topic);
    }
    if (auto status = metadata_.refresh(topics); !status.ok()) {
        return {status.error(), status.message()};
    }
    if (!cfg_.assignment.empty()) {
        return cfg_.assignment;
    }
    std::vector<TopicPartition> partitions;
    for (const std::string& topic : cfg_.topics) {
        const auto count = metadata_.partitionCount(topic).value_or(0);
        for (std::uint32_t p = 0; p < count; ++p) {
            partitions.push_back({topic, p});
        }
    }
    if (partitions.empty()) {
        return {ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, "nothing to consume"};
    }
    return partitions;
}

}  // namespace elog
