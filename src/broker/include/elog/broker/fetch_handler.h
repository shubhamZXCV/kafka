#pragma once

#include "elog/broker/broker_config.h"
#include "elog/broker/hw_notifier.h"
#include "elog/broker/replica_manager.h"
#include "elog/protocol/messages.h"

namespace elog {

// Serves consumer Fetch (LLD §6.6): committed records only, never more than the request's
// credit in total, and a long-poll of up to maxWaitMs when there is nothing to return.
//
// Thread rules: handle() is called concurrently by connection threads and blocks the caller
// during the long-poll.
class FetchHandler {
public:
    FetchHandler(ReplicaManager& replicas, HwNotifier& hwNotifier, const BrokerConfig& cfg);

    FetchResponse handle(const FetchRequest& request);

private:
    // One pass over the partitions; adds the records returned to recordsReturned.
    FetchResponse readOnce(const FetchRequest& request, std::uint32_t& recordsReturned);

    ReplicaManager& replicas_;
    HwNotifier& hwNotifier_;
    const BrokerConfig& cfg_;
};

}  // namespace elog
