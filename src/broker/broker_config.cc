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
        ConfigKey::integer("max.frame.bytes", 8'388'608, 1024, 1 << 30),
        ConfigKey::integer("max.connections", 256, 1, 65535),
        ConfigKey::integer("connection.idle.ms", 300'000, 1, kOneDayMs),
        ConfigKey::integer("request.timeout.ms", 30'000, 1, kOneDayMs),
        ConfigKey::integer("max.pending.produce", 64, 1, kMaxPendingProduce),
        ConfigKey::integer("produce.admission.wait.ms", 5'000, 0, kOneDayMs),
        ConfigKey::integer("produce.throttle.hint.ms", 100, 1, 60'000),
        ConfigKey::integer("fetch.max.bytes", 1'048'576, 1024, 1 << 30),
        ConfigKey::integer("fetch.max.wait.ms", 500, 0, 60'000),
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
    cfg.maxFrameBytes = static_cast<std::size_t>(config.getInt("max.frame.bytes"));
    cfg.maxConnections = static_cast<int>(config.getInt("max.connections"));
    cfg.connectionIdle = config.getMs("connection.idle.ms");
    cfg.requestTimeout = config.getMs("request.timeout.ms");
    cfg.maxPendingProduce = static_cast<std::ptrdiff_t>(config.getInt("max.pending.produce"));
    cfg.produceAdmissionWait = config.getMs("produce.admission.wait.ms");
    cfg.produceThrottleHint = config.getMs("produce.throttle.hint.ms");
    cfg.fetchMaxBytes = static_cast<std::size_t>(config.getInt("fetch.max.bytes"));
    cfg.fetchMaxWait = config.getMs("fetch.max.wait.ms");
    return cfg;
}

}  // namespace elog
