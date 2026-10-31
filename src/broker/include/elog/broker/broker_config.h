#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "elog/common/config.h"
#include "elog/common/types.h"

namespace elog {

// The broker's configuration keys (LLD §12.1). Keys are added here as the code that reads
// them lands, so an unsupported key is still a startup error.
std::vector<ConfigKey> brokerConfigSchema();

// The broker settings, typed. Built once at startup; immutable afterwards.
struct BrokerConfig {
    BrokerId brokerId = 0;
    std::string listenHost;
    std::uint16_t listenPort = 0;
    std::size_t maxFrameBytes = 0;
    int maxConnections = 0;
    std::chrono::milliseconds connectionIdle{0};  // receive timeout on client connections
    std::chrono::milliseconds requestTimeout{0};  // send timeout on client connections (R-10)

    // `config` must have been loaded with brokerConfigSchema().
    static BrokerConfig from(const Config& config);
};

}  // namespace elog
