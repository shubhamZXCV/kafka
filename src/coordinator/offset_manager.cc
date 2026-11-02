#include "elog/coordinator/offset_manager.h"

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/group_codec.h"

namespace elog {

OffsetManager::OffsetManager(Repository& repo, EventLog& events, Metrics& metrics,
                             bool fencingEnabled)
    : repo_(repo), events_(events), metrics_(metrics), fencingEnabled_(fencingEnabled) {}

// LLD §7.4: every partition of one request in one transaction (one fsync per request).
Frame OffsetManager::onCommit(const Frame& request, TimeMs nowWallMs) {
    auto commit = decodeOffsetCommitRequest(request);
    if (!commit.ok()) {
        return encodeErrorResponse(request, commit.error());
    }
    const OffsetCommitRequest& r = commit.value();
    OffsetCommitResponse response;
    auto txn = repo_.begin();
    Status status = txn.ok() ? okStatus() : Status{txn.error(), txn.message()};
    for (const OffsetCommitEntry& e : r.offsets) {
        ErrorCode error = ErrorCode::UNKNOWN_SERVER_ERROR;
        if (status.ok()) {
            status = commitEntry(r, e, nowWallMs, error);
        }
        response.offsets.push_back({e.topic, e.partition, error});
    }
    if (status.ok()) {
        status = txn.value().commit();
    }
    if (!status.ok()) {  // nothing was stored (the transaction rolled back)
        logging::get("coordinator")
            ->critical("offset_commit_failed group={} error=\"{}\"", r.groupId, status.message());
        for (auto& e : response.offsets) {
            e.error = ErrorCode::UNKNOWN_SERVER_ERROR;
        }
    }
    return encode(response, request.correlationId);
}

Status OffsetManager::commitEntry(const OffsetCommitRequest& r, const OffsetCommitEntry& e,
                                  TimeMs nowWallMs, ErrorCode& error) {
    const TopicPartition tp{e.topic, e.partition};
    if (fencingEnabled_) {
        auto accepted = repo_.commitOffsetFenced(r.groupId, tp, e.offset, e.epoch, r.memberId,
                                                 nowWallMs);  // LLD §8.3, one statement
        if (!accepted.ok()) {
            return {accepted.error(), accepted.message()};
        }
        if (!accepted.value()) {
            metrics_.counter("fence_rejections_total", "group=" + r.groupId).add();
            events_.emit({.event = "commit_fenced",
                          .group = r.groupId,
                          .tp = tp,
                          .epoch = e.epoch,
                          .member = r.memberId,
                          .offset = e.offset});
            error = ErrorCode::STALE_EPOCH;
            return okStatus();
        }
    } else {
        auto previous = repo_.committedOffset(r.groupId, tp);  // read only for detection
        if (!previous.ok()) {
            return {previous.error(), previous.message()};
        }
        if (auto status = repo_.commitOffsetUnfenced(r.groupId, tp, e.offset, e.epoch, nowWallMs);
            !status.ok()) {
            return {status.error(), status.message()};
        }
        if (previous.value() && e.offset < *previous.value()) {
            metrics_.counter("offset_regressions_total", "group=" + r.groupId).add();
            events_.emit({.event = "offset_regression",
                          .group = r.groupId,
                          .tp = tp,
                          .epoch = e.epoch,
                          .member = r.memberId,
                          .offset = e.offset,
                          .detail = "prev=" + std::to_string(*previous.value())});
        }
    }
    metrics_.counter("commits_total", "group=" + r.groupId).add();
    events_.emit({.event = "commit_accepted",
                  .group = r.groupId,
                  .tp = tp,
                  .epoch = e.epoch,
                  .member = r.memberId,
                  .offset = e.offset});
    error = ErrorCode::NONE;
    return okStatus();
}

void OffsetManager::onTick(TimeMs now) {
    if (!fencingEnabled_ && now - lastFencingWarningMs_ >= 10'000) {  // LLD §12.2
        logging::get("coordinator")->warn("FENCING_DISABLED offset commits are not fenced");
        lastFencingWarningMs_ = now;
    }
}

Frame OffsetManager::onFetch(const Frame& request) {
    auto fetch = decodeOffsetFetchRequest(request);
    if (!fetch.ok()) {
        return encodeErrorResponse(request, fetch.error());
    }
    OffsetFetchResponse response;
    for (const TopicPartition& tp : fetch.value().partitions) {
        auto offset = repo_.committedOffset(fetch.value().groupId, tp);
        if (!offset.ok()) {
            response.offsets.push_back(
                {tp.topic, tp.partition, -1, ErrorCode::UNKNOWN_SERVER_ERROR});
        } else {
            const auto value = offset.value() ? static_cast<std::int64_t>(*offset.value()) : -1;
            response.offsets.push_back({tp.topic, tp.partition, value, ErrorCode::NONE});
        }
    }
    return encode(response, request.correlationId);
}

}  // namespace elog
