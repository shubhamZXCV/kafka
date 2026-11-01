#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "elog/common/config.h"
#include "elog/common/types.h"
#include "elog/storage/partition_log.h"

namespace elog {

// The broker's configuration keys (LLD §12.1). Keys are added here as the code that reads
// them lands, so an unsupported key is still a startup error.
std::vector<ConfigKey> brokerConfigSchema();

// The broker settings, typed. Built once at startup; immutable afterwards.
struct BrokerConfig {
    BrokerId brokerId = 0;
    std::string listenHost;
    std::uint16_t listenPort = 0;
    std::string dataDir;             // one subdirectory per partition replica
    std::string advertisedHost;      // the host clients and the coordinator connect to
    std::string coordinatorAddress;  // "host:port"
    std::chrono::milliseconds brokerHeartbeatInterval{0};
    LogConfig log;  // segment.bytes, segment.ms, index.interval.bytes, log.flush.policy
    std::chrono::milliseconds logFlushInterval{0};
    std::chrono::milliseconds recoveryCheckpointInterval{0};
    std::chrono::milliseconds hwCheckpointInterval{0};
    std::size_t maxFrameBytes = 0;
    int maxConnections = 0;
    std::chrono::milliseconds connectionIdle{0};  // receive timeout on client connections
    std::chrono::milliseconds requestTimeout{0};  // send timeout on client connections (R-10)
    std::ptrdiff_t maxPendingProduce = 0;         // admission slots per partition
    std::chrono::milliseconds produceAdmissionWait{0};
    std::chrono::milliseconds produceThrottleHint{0};  // throttleMs sent with THROTTLED (R-12)
    std::size_t fetchMaxBytes = 0;                     // caps a fetch's maxBytes
    std::chrono::milliseconds fetchMaxWait{0};         // caps a fetch's maxWaitMs

    // `config` must have been loaded with brokerConfigSchema().
    static BrokerConfig from(const Config& config);
};

}  // namespace elog
