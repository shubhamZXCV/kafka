#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include <spdlog/logger.h>

#include "elog/broker/broker_config.h"
#include "elog/broker/request_dispatcher.h"
#include "elog/common/socket.h"

namespace elog {

// The broker process: accepts client connections and serves each one on its own thread
// (LLD §6.1–6.2). Each connection carries one request at a time (LLD §4.2), so a slow
// request blocks only its own client, and a client that stops reading feels TCP
// backpressure.
//
// Thread rules: run() owns the calling thread (the accept loop). Every connection thread
// shares the dispatcher; the connection counters are atomic.
class BrokerServer {
public:
    explicit BrokerServer(BrokerConfig cfg);

    // Binds the listener and accepts connections. Returns a process exit code; for now it
    // only returns if the listener cannot be bound.
    int run();

private:
    void acceptLoop(Listener& listener);
    void serveConnection(Socket socket, std::uint64_t connectionId);

    const BrokerConfig cfg_;
    RequestDispatcher dispatcher_;
    std::shared_ptr<spdlog::logger> log_;
    std::atomic<int> activeConnections_{0};
    std::atomic<std::uint64_t> nextConnectionId_{1};  // for log lines only
};

}  // namespace elog
