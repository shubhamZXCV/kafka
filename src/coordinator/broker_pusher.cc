#include "elog/coordinator/broker_pusher.h"

#include "elog/common/logging.h"
#include "elog/common/socket.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/json_codec.h"

namespace elog {

namespace {

constexpr std::size_t kMaxPendingPushes = 10'000;
constexpr auto kConnectTimeout = std::chrono::milliseconds(1000);
constexpr auto kRequestTimeout = std::chrono::milliseconds(5000);
constexpr std::size_t kMaxResponseBytes = 1 << 20;

}  // namespace

BrokerPusher::BrokerPusher(BoundedQueue<Event>& results)
    : results_(results), pending_(kMaxPendingPushes) {}

void BrokerPusher::start() {
    thread_ = std::thread([this] { run(); });
}

void BrokerPusher::enqueue(LeaderAndIsrPush push) {
    const BrokerId broker = push.target.id;
    if (!pending_.push(std::move(push), std::chrono::milliseconds(0))) {
        // Lost pushes are recovered: a broker without its roles re-registers (R-15).
        logging::get("coordinator")->error("push_dropped broker={} reason=queue_full", broker);
    }
}

void BrokerPusher::stop() {
    pending_.close();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void BrokerPusher::run() {
    while (true) {
        auto push = pending_.pop(std::chrono::milliseconds(100));
        if (!push) {
            if (pending_.closed()) {
                return;
            }
            continue;
        }
        const ErrorCode error = deliver(*push);
        if (error == ErrorCode::NONE) {
            continue;
        }
        logging::get("coordinator")
            ->warn("push_failed broker={} error={}", push->target.id, errorName(error));
        // Bounded wait: if the loop is gone (shutdown) the result is simply dropped.
        results_.push(PushResultEvent{push->target.id, push->incarnation, error},
                      std::chrono::milliseconds(1000));
    }
}

// A fresh connection per push: pushes are rare (registration, topic creation, failover).
ErrorCode BrokerPusher::deliver(const LeaderAndIsrPush& push) {
    auto socket = Socket::connect(push.target.host, push.target.port, kConnectTimeout);
    if (!socket.ok()) {
        return socket.error();
    }
    if (!socket.value().setTimeouts(kRequestTimeout, kRequestTimeout).ok()) {
        return ErrorCode::NETWORK_ERROR;
    }
    if (auto status = writeFrame(socket.value(), encode(push.request, nextCorrelationId_++));
        !status.ok()) {
        return status.error();
    }
    auto frame = readFrame(socket.value(), kMaxResponseBytes);
    if (!frame.ok()) {
        return frame.error();
    }
    auto response = decodeLeaderAndIsrResponse(frame.value());
    return response.ok() ? response.value().error : response.error();
}

}  // namespace elog
