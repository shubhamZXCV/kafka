#include "elog/coordinator/event_loop.h"

#include <algorithm>
#include <exception>
#include <variant>

#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/error_response.h"

namespace elog {

namespace {

constexpr TimeMs kPushRetryDelayMs = 500;  // R-15

}  // namespace

EventLoop::EventLoop(const CoordinatorConfig& cfg, Repository& repo, BrokerPusher& pusher,
                     EventLog& events)
    : tick_(cfg.tick),
      meta_(repo),
      brokers_(repo, cfg.brokerSessionTimeout),
      groups_(repo, meta_, events, cfg.groups),
      offsets_(repo, events),
      pusher_(pusher),
      controllerIncarnation_(static_cast<std::uint32_t>(time::wallMs() / 1000)) {}

Status EventLoop::load() {
    if (auto status = meta_.load(); !status.ok()) {
        return status;
    }
    return groups_.load();
}

// R-13: wait for a request only until the next tick is due, and check the tick deadline
// after every request, so ticks never wait behind a full queue.
void EventLoop::run(BoundedQueue<LoopEvent>& inbound) {
    using Clock = std::chrono::steady_clock;
    auto nextTick = Clock::now() + tick_;
    while (true) {
        const auto untilTick =
            std::chrono::duration_cast<std::chrono::milliseconds>(nextTick - Clock::now());
        auto event = inbound.pop(std::max(untilTick, std::chrono::milliseconds(0)));
        if (event) {
            std::visit([this](auto& e) { handle(e); }, *event);
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
        if (event.frame.apiKey == ApiKey::JoinGroup) {  // answered when the rebalance ends
            groups_.onJoin(event.frame, event.reply, time::monotonicMs());
            return;
        }
        event.reply->complete(dispatch(event.frame));
        pushNewRoles();
    } catch (const std::exception& e) {
        logging::get("coordinator")
            ->error("handler_failed api={} correlation={} error=\"{}\"",
                    apiKeyName(event.frame.apiKey), event.frame.correlationId, e.what());
        event.reply->complete(encodeErrorResponse(event.frame, ErrorCode::UNKNOWN_SERVER_ERROR));
    }
}

// R-15: retry later with the broker's current state, while it lives in the same
// incarnation. A broker that re-registered gets a full push anyway.
void EventLoop::handle(const PushResultEvent& event) {
    pushRetries_[event.broker] = {event.incarnation, time::monotonicMs() + kPushRetryDelayMs};
}

void EventLoop::handle(const ShutdownEvent&) {
    groups_.failPendingJoins();
}

// R-15: a registered broker gets every partition it hosts (possibly none: it then knows it
// has nothing to serve); each replica of a new topic gets that topic's partitions.
void EventLoop::pushNewRoles() {
    for (BrokerId broker : brokers_.takeRegistered()) {
        push(broker, nullptr);
    }
    for (const std::string& topic : meta_.takeCreatedTopics()) {
        for (const BrokerAddress& broker : brokers_.liveBrokers()) {
            push(broker.id, &topic);
        }
    }
}

void EventLoop::push(BrokerId broker, const std::string* topic) {
    const auto live = brokers_.live(broker);
    if (!live) {
        return;  // it gets everything when it registers again
    }
    auto partitions = meta_.partitionsHostedBy(broker, topic);
    if (topic && partitions.empty()) {
        return;
    }
    pusher_.enqueue({live->address,
                     live->incarnation,
                     {controllerIncarnation_, std::move(partitions), brokers_.liveBrokers()}});
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
        case ApiKey::Heartbeat:
            return groups_.onHeartbeat(request, time::monotonicMs());
        case ApiKey::LeaveGroup:
            return groups_.onLeave(request, time::monotonicMs());
        case ApiKey::OffsetCommit:
            return offsets_.onCommit(request, time::wallMs());
        case ApiKey::OffsetFetch:
            return offsets_.onFetch(request);
        case ApiKey::DescribeGroup:
            return groups_.onDescribe(request);
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
    groups_.onTick(now);
    for (auto it = pushRetries_.begin(); it != pushRetries_.end();) {
        if (it->second.dueMs > now) {
            ++it;
            continue;
        }
        const auto live = brokers_.live(it->first);
        if (live && live->incarnation == it->second.incarnation) {
            push(it->first, nullptr);
        }
        it = pushRetries_.erase(it);
    }
}

}  // namespace elog
