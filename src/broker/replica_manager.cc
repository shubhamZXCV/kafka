#include "elog/broker/replica_manager.h"

#include <string>
#include <utility>

#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/error_codes.h"
#include "elog/storage/checkpoint_file.h"
#include "elog/storage/partition_log.h"

namespace elog {

namespace {

// LLD §5.6: a missing or unreadable checkpoint only means recovery scans more of the log.
std::map<TopicPartition, Offset> loadRecoveryPoints(const std::string& dataDir) {
    auto points = CheckpointFile::read(dataDir + "/recovery-point.checkpoint");
    if (!points.ok()) {
        logging::get("storage")->warn("recovery_checkpoint_ignored error=\"{}\"", points.message());
        return {};
    }
    return std::move(points).value();
}

}  // namespace

ReplicaManager::ReplicaManager(std::string dataDir, LogConfig logConfig,
                               std::ptrdiff_t maxPendingProduce, HwNotifier& hwNotifier)
    : dataDir_(std::move(dataDir)),
      logConfig_(logConfig),
      recoveryPoints_(loadRecoveryPoints(dataDir_)),
      maxPendingProduce_(maxPendingProduce),
      hwNotifier_(hwNotifier) {}

std::shared_ptr<Replica> ReplicaManager::get(const TopicPartition& tp) const {
    std::shared_lock lock(mapMu_);  // lock order: ReplicaManager::mapMu_ (LLD §11.2)
    const auto it = replicas_.find(tp);
    return it == replicas_.end() ? nullptr : it->second;
}

// LLD §6.3, R-11: creators queue on createMu_ so two of them never open the same log
// directory, while lookups by produce and fetch only ever wait for the brief map insert.
Result<std::shared_ptr<Replica>> ReplicaManager::getOrCreate(const TopicPartition& tp) {
    if (auto replica = get(tp)) {
        return replica;
    }
    std::lock_guard createLock(createMu_);  // lock order: createMu_ → mapMu_ (LLD §11.2)
    if (auto replica = get(tp)) {
        return replica;  // another creator got here first
    }
    const std::string dir = dataDir_ + "/" + tp.topic + "-" + std::to_string(tp.partition);
    auto log = PartitionLog::open(dir, logConfig_, recoveryPoints_[tp]);
    if (!log.ok()) {
        logging::get("broker")->error("replica_open_failed topic={} partition={} error=\"{}\"",
                                      tp.topic, tp.partition, log.message());
        return {log.error(), log.message()};
    }
    auto replica =
        std::make_shared<Replica>(tp, std::move(log).value(), maxPendingProduce_, hwNotifier_);
    {
        std::unique_lock mapLock(mapMu_);  // lock order: createMu_ → mapMu_ (LLD §11.2)
        replicas_.emplace(tp, replica);
    }
    logging::get("broker")->info("replica_created topic={} partition={} dir={}", tp.topic,
                                 tp.partition, dir);
    return replica;
}

LeaderAndIsrResponse ReplicaManager::applyLeaderAndIsr(const LeaderAndIsrRequest& request,
                                                       BrokerId self) {
    LeaderAndIsrResponse response;
    for (const PartitionState& state : request.partitions) {
        PartitionError result{state.topic, state.partition, ErrorCode::NONE};
        auto replica = getOrCreate({state.topic, state.partition});
        result.error =
            replica.ok() ? replica.value()->applyLeaderAndIsr(state, self) : replica.error();
        if (result.error != ErrorCode::NONE) {
            logging::get("broker")->warn(
                "leader_and_isr_rejected topic={} partition={} epoch={} error={}", state.topic,
                state.partition, state.leaderEpoch, errorName(result.error));
        }
        response.partitions.push_back(std::move(result));
    }
    logging::get("broker")->info("leader_and_isr_applied controller={} partitions={}",
                                 request.controllerIncarnation, request.partitions.size());
    return response;
}

std::vector<std::shared_ptr<Replica>> ReplicaManager::all() const {
    std::shared_lock lock(mapMu_);  // lock order: ReplicaManager::mapMu_ (LLD §11.2)
    std::vector<std::shared_ptr<Replica>> replicas;
    replicas.reserve(replicas_.size());
    for (const auto& [tp, replica] : replicas_) {
        replicas.push_back(replica);
    }
    return replicas;
}

void ReplicaManager::flushLogs() {
    for (const auto& replica : all()) {
        if (auto status = replica->flushLog(); !status.ok()) {
            const TopicPartition& tp = replica->topicPartition();
            logging::get("storage")->error("flush_failed topic={} partition={} error=\"{}\"",
                                           tp.topic, tp.partition, status.message());
        }
    }
}

void ReplicaManager::checkpointRecoveryPoints() {
    std::map<TopicPartition, Offset> offsets;
    for (const auto& replica : all()) {
        offsets[replica->topicPartition()] = replica->recoveryPoint();
    }
    const std::string path = dataDir_ + "/recovery-point.checkpoint";
    if (auto status = CheckpointFile::write(path, offsets); !status.ok()) {
        logging::get("storage")->error("checkpoint_failed file={} error=\"{}\"", path,
                                       status.message());
    }
}

void ReplicaManager::checkpointHighWatermarks() {
    std::map<TopicPartition, Offset> offsets;
    for (const auto& replica : all()) {
        offsets[replica->topicPartition()] = replica->hw();
    }
    const std::string path = dataDir_ + "/high-watermark.checkpoint";
    if (auto status = CheckpointFile::write(path, offsets); !status.ok()) {
        logging::get("storage")->error("checkpoint_failed file={} error=\"{}\"", path,
                                       status.message());
    }
}

void ReplicaManager::applyRetention() {
    for (const auto& replica : all()) {
        replica->applyRetention(time::wallMs());
    }
}

std::vector<IsrProposal> ReplicaManager::maybeShrinkIsrs(std::chrono::milliseconds lagTimeMax) {
    std::vector<IsrProposal> proposals;
    for (const auto& replica : all()) {
        if (auto proposal = replica->maybeShrinkIsr(time::monotonicMs(), lagTimeMax)) {
            proposals.push_back(std::move(*proposal));
        }
    }
    return proposals;
}

void ReplicaManager::sampleGauges(Metrics& metrics) const {
    for (const auto& replica : all()) {
        const TopicPartition& tp = replica->topicPartition();
        const std::string labels =
            "topic=" + tp.topic + ";partition=" + std::to_string(tp.partition);
        const ReplicaGauges g = replica->gauges();
        metrics.gauge("log_end_offset", labels).set(static_cast<std::int64_t>(g.leo));
        metrics.gauge("high_watermark", labels).set(static_cast<std::int64_t>(g.hw));
        metrics.gauge("log_size_bytes", labels).set(static_cast<std::int64_t>(g.logSizeBytes));
        metrics.gauge("isr_size", labels).set(static_cast<std::int64_t>(g.isrSize));
        metrics.gauge("pending_produce", labels).set(g.pendingProduce);
    }
}

}  // namespace elog
