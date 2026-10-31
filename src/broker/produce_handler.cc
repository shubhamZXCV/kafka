#include "elog/broker/produce_handler.h"

#include "elog/common/logging.h"

namespace elog {

ProduceHandler::ProduceHandler(ReplicaManager& replicas, const BrokerConfig& cfg)
    : replicas_(replicas), cfg_(cfg) {}

ProduceResponse ProduceHandler::handle(ProduceRequest& request) {
    ProduceResponse response;
    const std::chrono::milliseconds timeout(request.timeoutMs);
    for (ProducePartition& partition : request.partitions) {
        response.partitions.push_back(producePartition(partition, timeout));
    }
    return response;
}

ProducePartitionResponse ProduceHandler::producePartition(ProducePartition& partition,
                                                          std::chrono::milliseconds timeout) {
    ProducePartitionResponse out{partition.topic, partition.partition, ErrorCode::NONE, 0, 0};
    const auto replica = replicas_.get({partition.topic, partition.partition});
    if (!replica) {
        out.error = ErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
        return out;
    }

    // 1. Bounded admission (FR-BP-04). The slot is held until this function returns.
    const auto slot = replica->tryAdmitProduce(cfg_.produceAdmissionWait);
    if (!slot) {
        logging::get("broker")->warn("produce_throttled topic={} partition={}", partition.topic,
                                     partition.partition);
        out.error = ErrorCode::THROTTLED;
        out.throttleMs = static_cast<std::uint32_t>(cfg_.produceThrottleHint.count());
        return out;
    }

    // 2. Append: role, epoch and minISR checks happen under the replica lock.
    const auto info = replica->appendAsLeader(partition.records, partition.leaderEpoch);
    if (!info.ok()) {
        out.error = info.error();
        return out;
    }

    // 3. Acknowledge only once committed (FR-PROD-03). appendAsLeader accepted the request's
    // epoch, so it is the epoch the batch was appended under.
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    const auto status =
        replica->awaitCommitted(info.value().lastOffset, deadline, partition.leaderEpoch);
    if (!status.ok()) {
        out.error = status.error();
        return out;
    }
    out.baseOffset = info.value().baseOffset;
    return out;
}

}  // namespace elog
