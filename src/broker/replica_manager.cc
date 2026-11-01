#include "elog/broker/replica_manager.h"

#include <string>
#include <utility>

#include "elog/common/logging.h"
#include "elog/protocol/error_codes.h"
#include "elog/storage/partition_log.h"

namespace elog {

ReplicaManager::ReplicaManager(std::string dataDir, std::ptrdiff_t maxPendingProduce,
                               HwNotifier& hwNotifier)
    : dataDir_(std::move(dataDir)),
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
    auto log = PartitionLog::open(dir);
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

}  // namespace elog
