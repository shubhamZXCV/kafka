#include "elog/coordinator/event_loop.h"

#include <algorithm>
#include <exception>

#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/error_response.h"

namespace elog {

EventLoop::EventLoop(std::chrono::milliseconds tick, Repository& repo) : tick_(tick), meta_(repo) {}

Status EventLoop::load() {
    return meta_.load();
}

// R-13: wait for a request only until the next tick is due, and check the tick deadline
// after every request, so ticks never wait behind a full queue.
void EventLoop::run(BoundedQueue<RequestEvent>& inbound) {
    using Clock = std::chrono::steady_clock;
    auto nextTick = Clock::now() + tick_;
    while (true) {
        const auto untilTick =
            std::chrono::duration_cast<std::chrono::milliseconds>(nextTick - Clock::now());
        auto event = inbound.pop(std::max(untilTick, std::chrono::milliseconds(0)));
        if (event) {
            handle(*event);
        } else if (inbound.closed()) {
            return;  // closed and drained
        }
        if (Clock::now() >= nextTick) {
            onTick(time::monotonicMs());
            nextTick = Clock::now() + tick_;
        }
    }
}

// LLD §7.2: a handler that throws must not take the loop down, and its client still gets
// an answer.
void EventLoop::handle(RequestEvent& event) {
    try {
        event.reply->complete(dispatch(event.frame));
    } catch (const std::exception& e) {
        logging::get("coordinator")
            ->error("handler_failed api={} correlation={} error=\"{}\"",
                    apiKeyName(event.frame.apiKey), event.frame.correlationId, e.what());
        event.reply->complete(encodeErrorResponse(event.frame, ErrorCode::UNKNOWN_SERVER_ERROR));
    }
}

Frame EventLoop::dispatch(const Frame& request) {
    const std::vector<BrokerAddress> liveBrokers;  // broker registration comes next
    switch (request.apiKey) {
        case ApiKey::Metadata:
            return meta_.onMetadata(request, liveBrokers);
        case ApiKey::CreateTopic:
            return meta_.onCreateTopic(request, liveBrokers);
        default:
            break;
    }
    logging::get("coordinator")
        ->warn("unhandled_request api={} correlation={}", apiKeyName(request.apiKey),
               request.correlationId);
    return encodeErrorResponse(request, ErrorCode::INVALID_REQUEST);
}

// Broker liveness and group session timeouts are checked here as they land.
void EventLoop::onTick(TimeMs) {}

}  // namespace elog
