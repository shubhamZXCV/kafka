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

BrokerServer::BrokerServer(BrokerConfig cfg) : cfg_(std::move(cfg)), log_(logging::get("broker")) {}

int BrokerServer::run() {
    auto listener = Listener::bind(cfg_.listenHost, cfg_.listenPort);
    if (!listener.ok()) {
        log_->error("listen_failed host={} port={} error=\"{}\"", cfg_.listenHost, cfg_.listenPort,
                    listener.message());
        return 1;
    }
    log_->info("listening host={} port={}", cfg_.listenHost, cfg_.listenPort);
    acceptLoop(listener.value());
    return 0;
}

// LLD §6.2. Connection threads are detached for now; the registry that lets shutdown join
// them comes with graceful shutdown.
void BrokerServer::acceptLoop(Listener& listener) {
    while (true) {
        auto socket = listener.accept();
        if (!socket.ok()) {
            log_->warn("accept_failed error=\"{}\"", socket.message());
            std::this_thread::sleep_for(kAcceptRetryDelay);
            continue;
        }
        const std::uint64_t id = nextConnectionId_++;
        if (activeConnections_.load() >= cfg_.maxConnections) {
            log_->warn("connection_rejected conn={} max_connections={}", id, cfg_.maxConnections);
            continue;  // the socket closes as it goes out of scope
        }
        ++activeConnections_;
        try {
            std::thread([this, s = std::move(socket).value(), id]() mutable {
                serveConnection(std::move(s), id);
                --activeConnections_;
            }).detach();
        } catch (const std::system_error& e) {  // could not start a thread
            --activeConnections_;
            log_->error("connection_thread_failed conn={} error=\"{}\"", id, e.what());
        }
    }
}

// Read a request, dispatch it, write the response, repeat. Any read error ends the
// connection: the peer closed it, it was idle for connection.idle.ms, or the frame was
// malformed (NFR-SEC-02: close without crashing).
void BrokerServer::serveConnection(Socket socket, std::uint64_t connectionId) {
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
