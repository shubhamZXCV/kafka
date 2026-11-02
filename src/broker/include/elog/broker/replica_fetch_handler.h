#pragma once

#include "elog/broker/broker_config.h"
#include "elog/broker/coordinator_agent.h"
#include "elog/broker/hw_notifier.h"
#include "elog/broker/replica_manager.h"
#include "elog/protocol/replication_messages.h"

namespace elog {

// Serves followers' ReplicaFetch on the leader (LLD §6.7): each partition's fetch offset
// acknowledges what the follower has, and the reply carries whole batches from there,
// long-polling up to replica.fetch.wait.ms when nothing new exists.
//
// Thread rules: handle() is called concurrently by connection threads.
class ReplicaFetchHandler {
public:
    ReplicaFetchHandler(ReplicaManager& replicas, HwNotifier& hwNotifier, CoordinatorAgent& agent,
                        const BrokerConfig& cfg);

    ReplicaFetchResponse handle(const ReplicaFetchRequest& request);

private:
    ReplicaFetchResponse readOnce(const ReplicaFetchRequest& request, bool& anyRecords);

    ReplicaManager& replicas_;
    HwNotifier& hwNotifier_;
    CoordinatorAgent& agent_;
    const BrokerConfig& cfg_;
};

}  // namespace elog
