#include "elog/coordinator/coordinator_server.h"

#include <memory>
#include <thread>
#include <utility>

#include "elog/common/logging.h"
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
    EventLoop loop(cfg_.tick, cfg_.brokerSessionTimeout, *repo.value());
    if (auto status = loop.load(); !status.ok()) {
        logging::get("coordinator")->error("load_failed error=\"{}\"", status.message());
        return 1;
    }
    std::thread loopThread([&] { loop.run(inbound_); });
    const int exitCode = server_.run();
    // Every connection thread has been joined, so nothing waits on the loop any more.
    inbound_.close();
    loopThread.join();
    if (exitCode == 0) {
        logging::get("coordinator")->info("stopped");
    }
    return exitCode;
}

void CoordinatorServer::shutdown() {
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
