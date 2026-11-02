#pragma once

#include "elog/broker/broker_config.h"
#include "elog/broker/broker_scheduler.h"
#include "elog/broker/coordinator_agent.h"
#include "elog/broker/fetch_handler.h"
#include "elog/broker/hw_notifier.h"
#include "elog/broker/produce_handler.h"
#include "elog/broker/replica_fetch_handler.h"
#include "elog/broker/replica_fetcher.h"
#include "elog/broker/replica_manager.h"
#include "elog/broker/request_dispatcher.h"
#include "elog/common/metrics.h"
#include "elog/protocol/frame_server.h"

namespace elog {

// The broker process: a FrameServer whose requests go to the RequestDispatcher, plus the
// partitions it hosts (LLD §6.1).
//
// Thread rules: run() owns the calling thread; shutdown() may be called from any thread.
class BrokerServer {
public:
    BrokerServer(BrokerConfig cfg, Metrics& metrics);

    // Serves until shutdown(). Returns a process exit code: 0 after shutdown, 1 if the
    // listener cannot be bound.
    int run();

    // Stops accepting and closes every connection, so run() returns (LLD §6.2).
    void shutdown();

private:
    void scheduleJobs();

    const BrokerConfig cfg_;
    Metrics& metrics_;
    HwNotifier hwNotifier_;
    ReplicaManager replicas_;
    CoordinatorAgent agent_;  // before the handlers that queue ISR proposals on it
    ProduceHandler produceHandler_;
    FetchHandler fetchHandler_;
    ReplicaFetchHandler replicaFetchHandler_;
    ReplicaFetcherManager fetchers_;
    RequestDispatcher dispatcher_;
    BrokerScheduler scheduler_;
    FrameServer server_;
};

}  // namespace elog
