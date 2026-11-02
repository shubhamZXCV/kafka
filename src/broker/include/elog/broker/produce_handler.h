#pragma once

#include "elog/broker/broker_config.h"
#include "elog/broker/replica_manager.h"
#include "elog/common/metrics.h"
#include "elog/protocol/messages.h"

namespace elog {

// Serves Produce (LLD §6.5): per partition, take an admission slot, append as leader, then
// wait until the batch is committed before acknowledging it.
//
// Thread rules: handle() is called concurrently by connection threads and blocks the caller
// while it waits for a slot or for the commit.
class ProduceHandler {
public:
    ProduceHandler(ReplicaManager& replicas, const BrokerConfig& cfg, Metrics& metrics);

    // Takes the request by reference because each batch is stamped in place.
    ProduceResponse handle(ProduceRequest& request);

private:
    ProducePartitionResponse producePartition(ProducePartition& partition,
                                              std::chrono::milliseconds timeout);

    ReplicaManager& replicas_;
    const BrokerConfig& cfg_;
    Counter& throttled_;  // produce_throttled_total
};

}  // namespace elog
