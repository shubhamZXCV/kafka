#include "elog/broker/broker_config.h"

namespace elog {

namespace {

constexpr std::int64_t kOneDayMs = 86'400'000;

}  // namespace

std::vector<ConfigKey> brokerConfigSchema() {
    return {
        ConfigKey::requiredInteger("broker.id", 0, INT32_MAX),
        ConfigKey::string("listen.host", "0.0.0.0"),
        ConfigKey::integer("listen.port", 9101, 1, 65535),
        ConfigKey::integer("max.frame.bytes", 8'388'608, 1024, 1 << 30),
        ConfigKey::integer("max.connections", 256, 1, 65535),
        ConfigKey::integer("connection.idle.ms", 300'000, 1, kOneDayMs),
        ConfigKey::integer("request.timeout.ms", 30'000, 1, kOneDayMs),
    };
}

BrokerConfig BrokerConfig::from(const Config& config) {
    BrokerConfig cfg;
    cfg.brokerId = static_cast<BrokerId>(config.getInt("broker.id"));
    cfg.listenHost = config.getString("listen.host");
    cfg.listenPort = static_cast<std::uint16_t>(config.getInt("listen.port"));
    cfg.maxFrameBytes = static_cast<std::size_t>(config.getInt("max.frame.bytes"));
    cfg.maxConnections = static_cast<int>(config.getInt("max.connections"));
    cfg.connectionIdle = config.getMs("connection.idle.ms");
    cfg.requestTimeout = config.getMs("request.timeout.ms");
    return cfg;
}

}  // namespace elog
