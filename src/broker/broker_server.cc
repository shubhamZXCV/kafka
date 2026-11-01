#include "elog/broker/broker_server.h"

#include <chrono>
#include <system_error>
#include <thread>
#include <utility>

#include "elog/common/logging.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/frame.h"

namespace elog {

namespace {

// After a failed accept() (e.g. out of file descriptors), wait a little instead of spinning.
constexpr auto kAcceptRetryDelay = std::chrono::milliseconds(100);

}  // namespace

BrokerServer::BrokerServer(BrokerConfig cfg)
    : cfg_(std::move(cfg)),
      replicas_(cfg_.dataDir, cfg_.maxPendingProduce, hwNotifier_),
      produceHandler_(replicas_, cfg_),
      fetchHandler_(replicas_, hwNotifier_, cfg_),
      dispatcher_(produceHandler_, fetchHandler_),
      log_(logging::get("broker")) {}

int BrokerServer::run() {
    auto listener = Listener::bind(cfg_.listenHost, cfg_.listenPort);
    if (!listener.ok()) {
        log_->error("listen_failed host={} port={} error=\"{}\"", cfg_.listenHost, cfg_.listenPort,
                    listener.message());
        return 1;
    }
    {
        std::lock_guard lock(connMu_);
        if (!running_) {
            return 0;  // shutdown() came first
        }
        listener_.emplace(std::move(listener).value());
    }
    log_->info("listening host={} port={}", cfg_.listenHost, cfg_.listenPort);
    acceptLoop();
    joinAll();
    log_->info("stopped");
    return 0;
}

// LLD §6.2: stop accepting, then shut every registered socket down, which wakes its thread
// from a blocked read. Threads busy in a handler finish the request first; every handler
// wait is bounded (admission wait, produce timeout, fetch long-poll).
void BrokerServer::shutdown() {
    std::lock_guard lock(connMu_);
    if (!running_) {
        return;
    }
    running_ = false;
    log_->info("shutting_down connections={}", connections_.size());
    if (listener_) {
        listener_->shutdown();
    }
    for (auto& [id, connection] : connections_) {
        connection.socket->shutdown();
    }
}

void BrokerServer::acceptLoop() {
    while (true) {
        auto socket = listener_->accept();
        if (!socket.ok()) {
            if (!isRunning()) {
                return;  // woken by shutdown()
            }
            log_->warn("accept_failed error=\"{}\"", socket.message());
            std::this_thread::sleep_for(kAcceptRetryDelay);
            continue;
        }
        if (!startConnection(std::move(socket).value())) {
            return;
        }
    }
}

// running_ is checked under connMu_, the lock shutdown() holds, so every connection either
// is registered before shutdown() runs (and gets shut down) or is never started.
bool BrokerServer::startConnection(Socket socket) {
    std::lock_guard lock(connMu_);
    if (!running_) {
        return false;  // the socket closes as it goes out of scope
    }
    reapFinished();
    const std::uint64_t id = nextConnectionId_++;
    if (connections_.size() >= static_cast<std::size_t>(cfg_.maxConnections)) {
        log_->warn("connection_rejected conn={} max_connections={}", id, cfg_.maxConnections);
        return true;
    }
    auto shared = std::make_shared<Socket>(std::move(socket));
    try {
        connections_[id].socket = shared;
        connections_[id].thread = std::thread([this, shared, id] {
            serveConnection(*shared, id);
            std::lock_guard doneLock(connMu_);
            if (auto it = connections_.find(id); it != connections_.end()) {
                it->second.finished = true;
            }
        });
    } catch (const std::system_error& e) {  // could not start a thread
        connections_.erase(id);
        log_->error("connection_thread_failed conn={} error=\"{}\"", id, e.what());
    }
    return true;
}

bool BrokerServer::isRunning() {
    std::lock_guard lock(connMu_);
    return running_;
}

// A finished thread has released connMu_ for the last time, so joining it here cannot
// deadlock.
void BrokerServer::reapFinished() {
    std::erase_if(connections_, [](auto& entry) {
        if (!entry.second.finished) {
            return false;
        }
        entry.second.thread.join();
        return true;
    });
}

// After acceptLoop() no connection is added. The threads are joined without connMu_, which
// each one takes as it finishes.
void BrokerServer::joinAll() {
    std::map<std::uint64_t, Connection> connections;
    {
        std::lock_guard lock(connMu_);
        connections.swap(connections_);
    }
    for (auto& [id, connection] : connections) {
        connection.thread.join();
    }
}

// Read a request, dispatch it, write the response, repeat. Any read error ends the
// connection: the peer closed it, it was idle for connection.idle.ms, or the frame was
// malformed (NFR-SEC-02: close without crashing).
void BrokerServer::serveConnection(Socket& socket, std::uint64_t connectionId) {
    if (auto status = socket.setTimeouts(cfg_.connectionIdle, cfg_.requestTimeout); !status.ok()) {
        log_->warn("connection_setup_failed conn={} error=\"{}\"", connectionId, status.message());
        return;
    }
    log_->debug("connection_opened conn={}", connectionId);
    while (true) {
        auto request = readFrame(socket, cfg_.maxFrameBytes);
        if (!request.ok()) {
            const bool malformed = request.error() != ErrorCode::NETWORK_ERROR;
            log_->log(malformed ? spdlog::level::warn : spdlog::level::debug,
                      "connection_closed conn={} error={} reason=\"{}\"", connectionId,
                      errorName(request.error()), request.message());
            return;
        }
        const Frame response = dispatcher_.dispatch(request.value());
        if (auto status = writeFrame(socket, response); !status.ok()) {
            log_->debug("connection_closed conn={} error={} reason=\"{}\"", connectionId,
                        errorName(status.error()), status.message());
            return;
        }
    }
}

}  // namespace elog
