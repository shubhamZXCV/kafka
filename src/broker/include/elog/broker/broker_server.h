#pragma once

#include "elog/broker/broker_config.h"
#include "elog/broker/coordinator_agent.h"
#include "elog/broker/fetch_handler.h"
#include "elog/broker/hw_notifier.h"
#include "elog/broker/produce_handler.h"
#include "elog/broker/replica_manager.h"
#include "elog/broker/request_dispatcher.h"
#include "elog/protocol/frame_server.h"

namespace elog {

// The broker process: a FrameServer whose requests go to the RequestDispatcher, plus the
// partitions it hosts (LLD §6.1).
//
// Thread rules: run() owns the calling thread; shutdown() may be called from any thread.
class BrokerServer {
public:
    explicit BrokerServer(BrokerConfig cfg);

    // Serves until shutdown(). Returns a process exit code: 0 after shutdown, 1 if the
    // listener cannot be bound.
    int run();

    // Stops accepting and closes every connection, so run() returns (LLD §6.2).
    void shutdown();

private:
    const BrokerConfig cfg_;
    HwNotifier hwNotifier_;
    ReplicaManager replicas_;
    ProduceHandler produceHandler_;
    FetchHandler fetchHandler_;
    RequestDispatcher dispatcher_;
    CoordinatorAgent agent_;
    FrameServer server_;
};

}  // namespace elog
