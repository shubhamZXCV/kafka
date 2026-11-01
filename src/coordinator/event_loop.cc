#include "elog/coordinator/event_loop.h"

#include <algorithm>
#include <exception>

#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/error_response.h"

namespace elog {

EventLoop::EventLoop(std::chrono::milliseconds tick, std::chrono::milliseconds brokerSessionTimeout,
                     Repository& repo)
    : tick_(tick), meta_(repo), brokers_(repo, brokerSessionTimeout) {}

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
    switch (request.apiKey) {
        case ApiKey::Metadata:
            return meta_.onMetadata(request, brokers_.liveBrokers());
        case ApiKey::CreateTopic:
            return meta_.onCreateTopic(request, brokers_.liveBrokers());
        case ApiKey::RegisterBroker:
            return brokers_.onRegister(request, time::monotonicMs());
        case ApiKey::BrokerHeartbeat:
            return brokers_.onHeartbeat(request, time::monotonicMs());
        default:
            break;
    }
    logging::get("coordinator")
        ->warn("unhandled_request api={} correlation={}", apiKeyName(request.apiKey),
               request.correlationId);
    return encodeErrorResponse(request, ErrorCode::INVALID_REQUEST);
}

// LLD §7.2. Leader election for dead brokers and group session timeouts hook in here.
void EventLoop::onTick(TimeMs now) {
    brokers_.onTick(now);
}

}  // namespace elog
