#include "elog/broker/fetch_handler.h"

#include <algorithm>
#include <chrono>

namespace elog {

FetchHandler::FetchHandler(ReplicaManager& replicas, HwNotifier& hwNotifier,
                           const BrokerConfig& cfg)
    : replicas_(replicas), hwNotifier_(hwNotifier), cfg_(cfg) {}

// LLD §6.6: read; if nothing came back, wait for any HW change and read again, until the
// deadline. Every wake-up re-reads, since the change may have been on another partition.
FetchResponse FetchHandler::handle(const FetchRequest& request) {
    const auto maxWait = std::min(std::chrono::milliseconds(request.maxWaitMs), cfg_.fetchMaxWait);
    const auto deadline = std::chrono::steady_clock::now() + maxWait;
    std::uint64_t seen = hwNotifier_.version();  // before reading, so no bump is missed
    while (true) {
        std::uint32_t recordsReturned = 0;
        FetchResponse response = readOnce(request, recordsReturned);
        if (recordsReturned > 0 || std::chrono::steady_clock::now() >= deadline) {
            return response;
        }
        seen = hwNotifier_.waitForChange(seen, deadline);
    }
}

FetchResponse FetchHandler::readOnce(const FetchRequest& request, std::uint32_t& recordsReturned) {
    FetchResponse response;
    std::uint32_t credit = request.credit;  // shared by all partitions (FR-FETCH-04)
    std::size_t bytesLeft = std::min<std::size_t>(request.maxBytes, cfg_.fetchMaxBytes);
    for (const FetchPartition& p : request.partitions) {
        FetchPartitionResponse out{p.topic, p.partition, ErrorCode::NONE, 0, 0, {}};
        const auto replica = replicas_.get({p.topic, p.partition});
        if (!replica) {
            out.error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
            response.partitions.push_back(std::move(out));
            continue;
        }
        auto data =
            replica->readForConsumer(p.fetchOffset, credit, bytesLeft, p.currentLeaderEpoch);
        if (!data.ok()) {
            out.error = data.error();
            response.partitions.push_back(std::move(out));
            continue;
        }
        credit -= data.value().recordCount;
        recordsReturned += data.value().recordCount;
        bytesLeft -= std::min(bytesLeft, data.value().records.size());
        out.highWatermark = data.value().highWatermark;
        out.logStartOffset = data.value().logStartOffset;
        out.records = std::move(data.value().records);
        response.partitions.push_back(std::move(out));
    }
    return response;
}

}  // namespace elog
