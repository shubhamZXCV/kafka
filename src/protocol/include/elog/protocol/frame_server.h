#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <spdlog/logger.h>

#include "elog/common/socket.h"
#include "elog/protocol/frame.h"

namespace elog {

struct FrameServerConfig {
    std::string listenHost;
    std::uint16_t listenPort = 0;
    std::size_t maxFrameBytes = 0;
    int maxConnections = 0;
    std::chrono::milliseconds connectionIdle{0};  // receive timeout on connections
    std::chrono::milliseconds requestTimeout{0};  // send timeout on connections (R-10)
};

// Accepts connections and serves each one on its own thread: read a frame, hand it to the
// handler, write the response, repeat (LLD §6.2). Each connection carries one request at a
// time (LLD §4.2), so a slow request blocks only its own client, and a client that stops
// reading feels TCP backpressure. Used by the broker and the coordinator.
//
// Thread rules: run() owns the calling thread (the accept loop); shutdown() may be called
// from any thread. The handler is called concurrently by every connection thread. connMu_
// guards running_ and the connection registry; it is never held while a request is served.
class FrameServer {
public:
    using Handler = std::function<Frame(const Frame&)>;

    // `component` names the logger, e.g. "broker".
    FrameServer(FrameServerConfig cfg, Handler handler, const std::string& component);

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

    const FrameServerConfig cfg_;
    const Handler handler_;
    std::shared_ptr<spdlog::logger> log_;
    std::optional<Listener> listener_;  // set once by run(), under connMu_

    std::mutex connMu_;
    bool running_ = true;
    std::map<std::uint64_t, Connection> connections_;  // the registry shutdown() closes
    std::uint64_t nextConnectionId_ = 1;               // for log lines only
};

}  // namespace elog
