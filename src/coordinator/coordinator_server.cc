#include "elog/coordinator/coordinator_server.h"

#include <memory>
#include <thread>
#include <utility>

#include "elog/common/logging.h"
#include "elog/coordinator/broker_pusher.h"
#include "elog/coordinator/event_loop.h"
#include "elog/protocol/error_response.h"

namespace elog {

CoordinatorServer::CoordinatorServer(CoordinatorConfig cfg)
    : cfg_(std::move(cfg)),
      inbound_(cfg_.inboundQueueCapacity),
      server_(
          cfg_.server, [this](const Frame& request) { return handle(request); }, "coordinator") {}

int CoordinatorServer::run() {
    auto repo = Repository::open(cfg_.dbPath);
    if (!repo.ok()) {
        logging::get("coordinator")
            ->error("db_open_failed path={} error=\"{}\"", cfg_.dbPath, repo.message());
        return 1;
    }
    auto events = EventLog::open(cfg_.metricsDir, "coordinator");
    if (!events.ok()) {
        logging::get("coordinator")
            ->error("events_open_failed dir={} error=\"{}\"", cfg_.metricsDir, events.message());
        return 1;
    }
    BrokerPusher pusher(inbound_);
    EventLoop loop(cfg_, *repo.value(), pusher, *events.value());
    if (auto status = loop.load(); !status.ok()) {
        logging::get("coordinator")->error("load_failed error=\"{}\"", status.message());
        return 1;
    }
    pusher.start();
    std::thread loopThread([&] { loop.run(inbound_); });
    const int exitCode = server_.run();
    // Every connection thread has been joined, so nothing waits on the loop any more.
    inbound_.close();
    loopThread.join();
    pusher.stop();
    if (exitCode == 0) {
        logging::get("coordinator")->info("stopped");
    }
    return exitCode;
}

// Connection threads waiting for a deferred JoinGroup reply only finish once the loop has
// answered it, so the loop is told first, and the answer usually reaches the client before
// its socket is shut down (R-20).
void CoordinatorServer::shutdown() {
    inbound_.push(ShutdownEvent{}, std::chrono::milliseconds(1000));
    server_.shutdown();
}

// LLD §7.2: the connection thread does I/O only. A queue that stays full for
// queue.admission.ms means the loop is overloaded, so the client is told to back off.
Frame CoordinatorServer::handle(const Frame& request) {
    auto reply = std::make_shared<ReplySlot>();
    if (!inbound_.push(RequestEvent{request, reply}, cfg_.queueAdmission)) {
        logging::get("coordinator")
            ->warn("request_throttled api={} correlation={}", apiKeyName(request.apiKey),
                   request.correlationId);
        return encodeErrorResponse(request, ErrorCode::THROTTLED);
    }
    return reply->wait();
}

}  // namespace elog
