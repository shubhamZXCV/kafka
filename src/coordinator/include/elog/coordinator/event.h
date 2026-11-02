#pragma once

#include <cstdint>
#include <memory>
#include <variant>

#include "elog/common/error_code.h"
#include "elog/common/types.h"
#include "elog/coordinator/reply_slot.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/messages.h"

namespace elog {

// One request, queued by a connection thread for the event loop (LLD §7.2).
struct RequestEvent {
    Frame frame;
    std::shared_ptr<ReplySlot> reply;
};

// One LeaderAndIsr delivery for the BrokerPusher (LLD §7.2).
struct LeaderAndIsrPush {
    BrokerAddress target;
    std::uint32_t incarnation = 0;  // the broker's incarnation when the push was made
    LeaderAndIsrRequest request;
};

// A delivery that failed, reported back to the event loop (R-15).
struct PushResultEvent {
    BrokerId broker = 0;
    std::uint32_t incarnation = 0;
    ErrorCode error = ErrorCode::NONE;
};

// Everything the event loop handles from its inbound queue.
using LoopEvent = std::variant<RequestEvent, PushResultEvent>;

}  // namespace elog
