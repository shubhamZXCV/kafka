#pragma once

#include "elog/coordinator/coordinator_config.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/frame_server.h"

namespace elog {

// The coordinator process: a FrameServer for brokers and clients (LLD §7.1).
//
// Thread rules: run() owns the calling thread; shutdown() may be called from any thread.
class CoordinatorServer {
public:
    explicit CoordinatorServer(CoordinatorConfig cfg);

    // Serves until shutdown(). Returns a process exit code: 0 after shutdown, 1 if the
    // listener cannot be bound.
    int run();

    // Stops accepting and closes every connection, so run() returns.
    void shutdown();

private:
    Frame handle(const Frame& request);  // called by every connection thread

    const CoordinatorConfig cfg_;
    FrameServer server_;
};

}  // namespace elog
