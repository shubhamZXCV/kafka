#include "elog/broker/broker_server.h"

#include <utility>

#include "elog/common/logging.h"
#include "elog/storage/checkpoint_file.h"

namespace elog {

namespace {

FrameServerConfig serverConfig(const BrokerConfig& cfg) {
    return {cfg.listenHost,     cfg.listenPort,     cfg.maxFrameBytes,
            cfg.maxConnections, cfg.connectionIdle, cfg.requestTimeout};
}

}  // namespace

BrokerServer::BrokerServer(BrokerConfig cfg, Metrics& metrics)
    : cfg_(std::move(cfg)),
      metrics_(metrics),
      replicas_(cfg_.dataDir, cfg_.log, cfg_.maxPendingProduce, hwNotifier_),
      agent_(cfg_, replicas_),
      produceHandler_(replicas_, cfg_, metrics_),
      fetchHandler_(replicas_, hwNotifier_, cfg_),
      replicaFetchHandler_(replicas_, hwNotifier_, agent_, cfg_),
      fetchers_(replicas_, cfg_),
      dispatcher_(produceHandler_, fetchHandler_, replicaFetchHandler_, replicas_, fetchers_,
                  cfg_.brokerId),
      server_(
          serverConfig(cfg_),
          [this](const Frame& request) { return dispatcher_.dispatch(request); }, "broker") {}

int BrokerServer::run() {
    // LLD §5.2.4: refuse to serve another broker's data directory.
    if (auto status = checkOrWriteMetaProperties(cfg_.dataDir, cfg_.brokerId); !status.ok()) {
        logging::get("broker")->critical("data_dir_rejected error=\"{}\"", status.message());
        return 1;
    }
    scheduleJobs();
    scheduler_.start();
    agent_.start();
    const int exitCode = server_.run();
    agent_.stop();
    fetchers_.stopAll();
    scheduler_.stop();
    // LLD §6.2: a clean stop leaves everything flushed and checkpointed.
    replicas_.flushLogs();
    replicas_.checkpointRecoveryPoints();
    replicas_.checkpointHighWatermarks();
    if (exitCode == 0) {
        logging::get("broker")->info("stopped");
    }
    return exitCode;
}

// LLD §6.10.
void BrokerServer::scheduleJobs() {
    if (cfg_.log.flushPolicy == FlushPolicy::INTERVAL) {
        scheduler_.every("flush", cfg_.logFlushInterval, [this] { replicas_.flushLogs(); });
    }
    scheduler_.every("recovery-point-checkpoint", cfg_.recoveryCheckpointInterval,
                     [this] { replicas_.checkpointRecoveryPoints(); });
    scheduler_.every("hw-checkpoint", cfg_.hwCheckpointInterval,
                     [this] { replicas_.checkpointHighWatermarks(); });
    scheduler_.every("retention", cfg_.retentionCheckInterval,
                     [this] { replicas_.applyRetention(); });
    scheduler_.every("metrics", cfg_.metricsFlushInterval,
                     [this] { replicas_.sampleGauges(metrics_); });
    scheduler_.every("isr-shrink", cfg_.replicaLagTimeMax / 2, [this] {  // LLD §6.10
        for (IsrProposal& proposal : replicas_.maybeShrinkIsrs(cfg_.replicaLagTimeMax)) {
            agent_.proposeIsr(std::move(proposal));
        }
    });
}

void BrokerServer::shutdown() {
    server_.shutdown();
}

}  // namespace elog
