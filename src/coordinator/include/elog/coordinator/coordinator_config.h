#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "elog/common/config.h"
#include "elog/protocol/frame_server.h"

namespace elog {

// The coordinator's configuration keys (LLD §12.2, R-13). Keys are added here as the code
// that reads them lands, so an unsupported key is still a startup error.
std::vector<ConfigKey> coordinatorConfigSchema();

// The coordinator settings, typed. Built once at startup; immutable afterwards.
struct CoordinatorConfig {
    FrameServerConfig server;           // listen address and connection limits
    std::chrono::milliseconds tick{0};  // event-loop tick period
    std::size_t inboundQueueCapacity = 0;
    std::chrono::milliseconds queueAdmission{0};  // wait for queue space, then THROTTLED

    // `config` must have been loaded with coordinatorConfigSchema().
    static CoordinatorConfig from(const Config& config);
};

}  // namespace elog
