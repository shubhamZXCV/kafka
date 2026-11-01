#include "elog/coordinator/coordinator_server.h"

#include <utility>

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"

namespace elog {

CoordinatorServer::CoordinatorServer(CoordinatorConfig cfg)
    : cfg_(std::move(cfg)),
      server_(
          cfg_.server, [this](const Frame& request) { return handle(request); }, "coordinator") {}

int CoordinatorServer::run() {
    const int exitCode = server_.run();
    if (exitCode == 0) {
        logging::get("coordinator")->info("stopped");
    }
    return exitCode;
}

void CoordinatorServer::shutdown() {
    server_.shutdown();
}

Frame CoordinatorServer::handle(const Frame& request) {
    // No handlers yet: every API key is unhandled for now (R-10).
    logging::get("coordinator")
        ->warn("unhandled_request api={} correlation={}", apiKeyName(request.apiKey),
               request.correlationId);
    return encodeErrorResponse(request, ErrorCode::INVALID_REQUEST);
}

}  // namespace elog
