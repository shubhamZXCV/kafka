#include "elog/broker/broker_config.h"

#include <unistd.h>

#include "elog/broker/replica.h"

namespace elog {

namespace {

constexpr std::int64_t kOneDayMs = 86'400'000;

}  // namespace

std::vector<ConfigKey> brokerConfigSchema() {
    return {
        ConfigKey::requiredInteger("broker.id", 0, INT32_MAX),
        ConfigKey::string("listen.host", "0.0.0.0"),
        ConfigKey::integer("listen.port", 9101, 1, 65535),
        ConfigKey::string("advertised.host", ""),
        ConfigKey::string("coordinator.address", "coordinator:9000"),
        ConfigKey::integer("broker.heartbeat.interval.ms", 2000, 100, 60'000),
        ConfigKey::string("data.dir", "./data"),
        ConfigKey::integer("segment.bytes", 67'108'864, 16 << 10, 1 << 30),  // R-19 lower bound
        ConfigKey::integer("segment.ms", 604'800'000, 1000, INT64_MAX),
        ConfigKey::integer("index.interval.bytes", 4096, 512, 1 << 20),
        ConfigKey::choice("log.flush.policy", "interval", {"batch", "interval", "none"}),
        ConfigKey::integer("log.flush.interval.ms", 1000, 10, kOneDayMs),
        ConfigKey::integer("recovery.checkpoint.interval.ms", 5000, 10, kOneDayMs),
        ConfigKey::integer("hw.checkpoint.interval.ms", 5000, 10, kOneDayMs),
        ConfigKey::integerOrDisabled("retention.bytes", -1, 16 << 10, INT64_MAX),
        ConfigKey::integerOrDisabled("retention.ms", -1, 1000, INT64_MAX),
        ConfigKey::integer("retention.check.interval.ms", 30'000, 10, kOneDayMs),
        ConfigKey::integer("max.frame.bytes", 8'388'608, 1024, 1 << 30),
        ConfigKey::integer("max.connections", 256, 1, 65535),
        ConfigKey::integer("connection.idle.ms", 300'000, 1, kOneDayMs),
        ConfigKey::integer("request.timeout.ms", 30'000, 1, kOneDayMs),
        ConfigKey::integer("max.pending.produce", 64, 1, kMaxPendingProduce),
        ConfigKey::integer("produce.admission.wait.ms", 5'000, 0, kOneDayMs),
        ConfigKey::integer("produce.throttle.hint.ms", 100, 1, 60'000),
        ConfigKey::integer("fetch.max.bytes", 1'048'576, 1024, 1 << 30),
        ConfigKey::integer("fetch.max.wait.ms", 500, 0, 60'000),
        ConfigKey::integer("replica.fetch.max.bytes", 4'194'304, 1024, 1 << 30),
        ConfigKey::integer("replica.fetch.wait.ms", 500, 0, 60'000),
        ConfigKey::integer("replica.lag.time.max.ms", 10'000, 1000, kOneDayMs),
    };
}

BrokerConfig BrokerConfig::from(const Config& config) {
    BrokerConfig cfg;
    cfg.brokerId = static_cast<BrokerId>(config.getInt("broker.id"));
    cfg.listenHost = config.getString("listen.host");
    cfg.listenPort = static_cast<std::uint16_t>(config.getInt("listen.port"));
    cfg.dataDir = config.getString("data.dir");
    cfg.advertisedHost = config.getString("advertised.host");
    if (cfg.advertisedHost.empty()) {  // LLD §12.1: defaults to this machine's hostname
        char name[256] = {};
        cfg.advertisedHost = ::gethostname(name, sizeof(name) - 1) == 0 ? name : "localhost";
    }
    cfg.coordinatorAddress = config.getString("coordinator.address");
    cfg.brokerHeartbeatInterval = config.getMs("broker.heartbeat.interval.ms");
    cfg.log.segmentBytes = static_cast<std::uint32_t>(config.getInt("segment.bytes"));
    cfg.log.segmentMs = config.getMs("segment.ms");
    cfg.log.indexIntervalBytes = static_cast<std::uint32_t>(config.getInt("index.interval.bytes"));
    const std::string policy = config.getString("log.flush.policy");
    cfg.log.flushPolicy = policy == "batch"  ? FlushPolicy::BATCH
                          : policy == "none" ? FlushPolicy::NONE
                                             : FlushPolicy::INTERVAL;
    cfg.logFlushInterval = config.getMs("log.flush.interval.ms");
    cfg.recoveryCheckpointInterval = config.getMs("recovery.checkpoint.interval.ms");
    cfg.hwCheckpointInterval = config.getMs("hw.checkpoint.interval.ms");
    cfg.log.retentionBytes = config.getInt("retention.bytes");
    cfg.log.retentionMs = config.getInt("retention.ms");
    cfg.retentionCheckInterval = config.getMs("retention.check.interval.ms");
    cfg.maxFrameBytes = static_cast<std::size_t>(config.getInt("max.frame.bytes"));
    cfg.maxConnections = static_cast<int>(config.getInt("max.connections"));
    cfg.connectionIdle = config.getMs("connection.idle.ms");
    cfg.requestTimeout = config.getMs("request.timeout.ms");
    cfg.maxPendingProduce = static_cast<std::ptrdiff_t>(config.getInt("max.pending.produce"));
    cfg.produceAdmissionWait = config.getMs("produce.admission.wait.ms");
    cfg.produceThrottleHint = config.getMs("produce.throttle.hint.ms");
    cfg.fetchMaxBytes = static_cast<std::size_t>(config.getInt("fetch.max.bytes"));
    cfg.fetchMaxWait = config.getMs("fetch.max.wait.ms");
    cfg.replicaFetchMaxBytes = static_cast<std::size_t>(config.getInt("replica.fetch.max.bytes"));
    cfg.replicaFetchWait = config.getMs("replica.fetch.wait.ms");
    cfg.replicaLagTimeMax = config.getMs("replica.lag.time.max.ms");
    return cfg;
}

}  // namespace elog
