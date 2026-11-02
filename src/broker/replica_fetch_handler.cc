#include "elog/broker/replica_fetch_handler.h"

#include <algorithm>
#include <chrono>

#include "elog/common/time.h"

namespace elog {

ReplicaFetchHandler::ReplicaFetchHandler(ReplicaManager& replicas, HwNotifier& hwNotifier,
                                         const BrokerConfig& cfg)
    : replicas_(replicas), hwNotifier_(hwNotifier), cfg_(cfg) {}

// Like the consumer fetch long-poll (LLD §6.6): the notifier version is read before the
// first pass, and every wake-up reads again.
ReplicaFetchResponse ReplicaFetchHandler::handle(const ReplicaFetchRequest& request) {
    const auto maxWait =
        std::min(std::chrono::milliseconds(request.maxWaitMs), cfg_.replicaFetchWait);
    const auto deadline = std::chrono::steady_clock::now() + maxWait;
    std::uint64_t seen = hwNotifier_.version();
    while (true) {
        bool anyRecords = false;
        ReplicaFetchResponse response = readOnce(request, anyRecords);
        if (anyRecords || std::chrono::steady_clock::now() >= deadline) {
            return response;
        }
        seen = hwNotifier_.waitForChange(seen, deadline);
    }
}

ReplicaFetchResponse ReplicaFetchHandler::readOnce(const ReplicaFetchRequest& request,
                                                   bool& anyRecords) {
    ReplicaFetchResponse response;
    std::size_t bytesLeft = std::min<std::size_t>(request.maxBytes, cfg_.replicaFetchMaxBytes);
    for (const ReplicaFetchPartition& p : request.partitions) {
        ReplicaFetchPartitionResponse out{p.topic, p.partition, ErrorCode::NONE, 0, 0, {}};
        const auto replica = replicas_.get({p.topic, p.partition});
        if (!replica) {
            out.error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
            response.partitions.push_back(std::move(out));
            continue;
        }
        auto data = replica->readForFollower(request.replicaId, p.fetchOffset, bytesLeft,
                                             p.currentLeaderEpoch, time::monotonicMs());
        if (!data.ok()) {
            out.error = data.error();
            response.partitions.push_back(std::move(out));
            continue;
        }
        out.highWatermark = data.value().highWatermark;
        out.leaderLeo = data.value().leaderLeo;
        out.records = std::move(data.value().records);
        anyRecords = anyRecords || !out.records.empty();
        bytesLeft -= std::min(bytesLeft, out.records.size());
        response.partitions.push_back(std::move(out));
    }
    return response;
}

}  // namespace elog
