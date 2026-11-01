#pragma once

#include "elog/common/bounded_queue.h"
#include "elog/coordinator/coordinator_config.h"
#include "elog/coordinator/event_loop.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/frame_server.h"

namespace elog {

// The coordinator process (LLD §7.1–7.2): a FrameServer whose connection threads queue each
// request on inbound_ and wait for its reply, and one event-loop thread that handles them.
//
// Thread rules: run() owns the calling thread and starts the loop thread; shutdown() may be
// called from any thread. Connection threads never touch coordinator state.
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
    BoundedQueue<RequestEvent> inbound_;
    EventLoop loop_;
    FrameServer server_;
};

}  // namespace elog
