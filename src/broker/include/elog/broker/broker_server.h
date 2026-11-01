#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#include <spdlog/logger.h>

#include "elog/broker/broker_config.h"
#include "elog/broker/fetch_handler.h"
#include "elog/broker/hw_notifier.h"
#include "elog/broker/produce_handler.h"
#include "elog/broker/replica_manager.h"
#include "elog/broker/request_dispatcher.h"
#include "elog/common/socket.h"

namespace elog {

// The broker process: accepts client connections and serves each one on its own thread
// (LLD §6.1–6.2). Each connection carries one request at a time (LLD §4.2), so a slow
// request blocks only its own client, and a client that stops reading feels TCP
// backpressure.
//
// Thread rules: run() owns the calling thread (the accept loop); shutdown() may be called
// from any thread. Every connection thread shares the dispatcher. connMu_ guards running_
// and the connection registry; it is not in the replica lock order and is never held while
// a request is served.
class BrokerServer {
public:
    explicit BrokerServer(BrokerConfig cfg);

    // Binds the listener and serves until shutdown(), then joins every connection thread.
    // Returns a process exit code: 0 after shutdown, 1 if the listener cannot be bound.
    int run();

    // Stops accepting and closes every connection, so run() returns (LLD §6.2). Safe more
    // than once, and before run().
    void shutdown();

private:
    struct Connection {
        std::shared_ptr<Socket> socket;  // shared so shutdown() can reach it during a read
        std::thread thread;
        bool finished = false;  // serveConnection returned; the thread can be joined
    };

    void acceptLoop();
    bool startConnection(Socket socket);  // false once shutting down
    void serveConnection(Socket& socket, std::uint64_t connectionId);
    bool isRunning();
    void reapFinished();  // requires connMu_
    void joinAll();

    const BrokerConfig cfg_;
    HwNotifier hwNotifier_;
    ReplicaManager replicas_;
    ProduceHandler produceHandler_;
    FetchHandler fetchHandler_;
    RequestDispatcher dispatcher_;
    std::shared_ptr<spdlog::logger> log_;
    std::optional<Listener> listener_;  // set once by run(), under connMu_

    std::mutex connMu_;
    bool running_ = true;
    std::map<std::uint64_t, Connection> connections_;  // the registry shutdown() closes
    std::uint64_t nextConnectionId_ = 1;               // for log lines only
};

}  // namespace elog
