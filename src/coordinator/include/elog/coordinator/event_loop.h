#pragma once

#include <chrono>
#include <memory>

#include "elog/common/bounded_queue.h"
#include "elog/common/types.h"
#include "elog/coordinator/reply_slot.h"
#include "elog/protocol/frame.h"

namespace elog {

// One request, queued by a connection thread for the event loop (LLD §7.2).
struct RequestEvent {
    Frame frame;
    std::shared_ptr<ReplySlot> reply;
};

// The single thread that owns all coordinator state (LLD §7.2, ADR-02). Connection threads
// only queue requests and wait for replies, so state needs no locks: requests are
// serialised instead.
//
// Thread rules: run() is the event-loop thread; nothing else calls into this class.
class EventLoop {
public:
    explicit EventLoop(std::chrono::milliseconds tick);

    // Handles requests and ticks until `inbound` is closed and drained.
    void run(BoundedQueue<RequestEvent>& inbound);

private:
    void handle(RequestEvent& event);
    Frame dispatch(const Frame& request);
    void onTick(TimeMs now);

    const std::chrono::milliseconds tick_;
};

}  // namespace elog
