#include "elog/broker/broker_server.h"

#include <utility>

#include "elog/common/logging.h"

namespace elog {

namespace {

FrameServerConfig serverConfig(const BrokerConfig& cfg) {
    return {cfg.listenHost,     cfg.listenPort,     cfg.maxFrameBytes,
            cfg.maxConnections, cfg.connectionIdle, cfg.requestTimeout};
}

}  // namespace

BrokerServer::BrokerServer(BrokerConfig cfg)
    : cfg_(std::move(cfg)),
      replicas_(cfg_.dataDir, cfg_.log, cfg_.maxPendingProduce, hwNotifier_),
      produceHandler_(replicas_, cfg_),
      fetchHandler_(replicas_, hwNotifier_, cfg_),
      dispatcher_(produceHandler_, fetchHandler_, replicas_, cfg_.brokerId),
      agent_(cfg_),
      server_(
          serverConfig(cfg_),
          [this](const Frame& request) { return dispatcher_.dispatch(request); }, "broker") {}

int BrokerServer::run() {
    agent_.start();
    const int exitCode = server_.run();
    agent_.stop();
    if (exitCode == 0) {
        logging::get("broker")->info("stopped");
    }
    return exitCode;
}

void BrokerServer::shutdown() {
    server_.shutdown();
}

}  // namespace elog
