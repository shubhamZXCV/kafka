#include "elog/coordinator/coordinator_config.h"

namespace elog {

namespace {

constexpr std::int64_t kOneDayMs = 86'400'000;

}  // namespace

std::vector<ConfigKey> coordinatorConfigSchema() {
    return {
        ConfigKey::string("listen.host", "0.0.0.0"),
        ConfigKey::integer("listen.port", 9000, 1, 65535),
        ConfigKey::integer("max.frame.bytes", 8'388'608, 1024, 1 << 30),
        ConfigKey::integer("max.connections", 256, 1, 65535),
        ConfigKey::integer("connection.idle.ms", 300'000, 1, kOneDayMs),
        ConfigKey::integer("request.timeout.ms", 30'000, 1, kOneDayMs),
    };
}

CoordinatorConfig CoordinatorConfig::from(const Config& config) {
    CoordinatorConfig cfg;
    cfg.server.listenHost = config.getString("listen.host");
    cfg.server.listenPort = static_cast<std::uint16_t>(config.getInt("listen.port"));
    cfg.server.maxFrameBytes = static_cast<std::size_t>(config.getInt("max.frame.bytes"));
    cfg.server.maxConnections = static_cast<int>(config.getInt("max.connections"));
    cfg.server.connectionIdle = config.getMs("connection.idle.ms");
    cfg.server.requestTimeout = config.getMs("request.timeout.ms");
    return cfg;
}

}  // namespace elog
