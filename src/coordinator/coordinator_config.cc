#include "elog/coordinator/coordinator_config.h"

namespace elog {

namespace {

constexpr std::int64_t kOneDayMs = 86'400'000;

}  // namespace

std::vector<ConfigKey> coordinatorConfigSchema() {
    return {
        ConfigKey::string("listen.host", "0.0.0.0"),
        ConfigKey::integer("listen.port", 9000, 1, 65535),
        ConfigKey::string("db.path", "./coordinator.db"),
        ConfigKey::string("metrics.dir", "./metrics"),
        ConfigKey::boolean("coordinator.fencing.enabled", true),  // false only for E5 control
        // R-20: roundrobin until the client keeps retained partitions running (C097).
        ConfigKey::choice("group.default.assignor", "roundrobin", {"roundrobin", "sticky"}),
        ConfigKey::integer("group.min.session.timeout.ms", 1000, 1, 3'600'000),
        ConfigKey::integer("group.max.session.timeout.ms", 60'000, 1, 3'600'000),
        ConfigKey::integer("coordinator.tick.ms", 100, 10, 1000),
        ConfigKey::integer("inbound.queue.capacity", 10'000, 1, 1'000'000),
        ConfigKey::integer("queue.admission.ms", 1000, 0, 60'000),
        ConfigKey::integer("broker.session.timeout.ms", 6000, 100, 600'000),
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
    cfg.dbPath = config.getString("db.path");
    cfg.metricsDir = config.getString("metrics.dir");
    cfg.fencingEnabled = config.getBool("coordinator.fencing.enabled");
    cfg.groups.defaultAssignor = config.getString("group.default.assignor");
    cfg.groups.minSessionTimeoutMs =
        static_cast<std::uint32_t>(config.getInt("group.min.session.timeout.ms"));
    cfg.groups.maxSessionTimeoutMs =
        static_cast<std::uint32_t>(config.getInt("group.max.session.timeout.ms"));
    cfg.tick = config.getMs("coordinator.tick.ms");
    cfg.inboundQueueCapacity = static_cast<std::size_t>(config.getInt("inbound.queue.capacity"));
    cfg.queueAdmission = config.getMs("queue.admission.ms");
    cfg.brokerSessionTimeout = config.getMs("broker.session.timeout.ms");
    return cfg;
}

}  // namespace elog
