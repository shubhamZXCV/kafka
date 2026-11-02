#include "elog/broker/coordinator_agent.h"

#include <algorithm>
#include <charconv>
#include <string>

#include "elog/common/logging.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/json_codec.h"
#include "elog/protocol/replication_codec.h"

namespace elog {

namespace {

constexpr auto kInitialBackoff = std::chrono::milliseconds(100);  // LLD §6.9
constexpr auto kMaxBackoff = std::chrono::milliseconds(5000);
constexpr auto kConnectTimeout = std::chrono::milliseconds(1000);
constexpr auto kRequestTimeout = std::chrono::milliseconds(5000);
constexpr std::size_t kMaxResponseBytes = 1 << 20;

}  // namespace

// "host:port". A malformed port stays 0, so connecting fails and is logged and retried.
CoordinatorAgent::CoordinatorAgent(const BrokerConfig& cfg, ReplicaManager& replicas)
    : cfg_(cfg), replicas_(replicas) {
    const std::string& address = cfg.coordinatorAddress;
    const auto colon = address.rfind(':');
    coordinatorHost_ = address.substr(0, colon);
    if (colon != std::string::npos) {
        std::from_chars(address.data() + colon + 1, address.data() + address.size(),
                        coordinatorPort_);
    }
}

void CoordinatorAgent::start() {
    thread_ = std::thread([this] { run(); });
}

void CoordinatorAgent::stop() {
    {
        std::lock_guard lock(mu_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void CoordinatorAgent::run() {
    auto backoff = kInitialBackoff;
    while (true) {
        if (incarnation_ == 0) {
            if (registerOnce()) {
                backoff = kInitialBackoff;
                continue;
            }
            if (!sleepFor(backoff)) {
                return;
            }
            backoff = std::min(backoff * 2, kMaxBackoff);
            continue;
        }
        // Wake early for ISR proposals; heartbeat once the interval has passed.
        const auto heartbeatDue = std::chrono::steady_clock::now() + cfg_.brokerHeartbeatInterval;
        while (std::chrono::steady_clock::now() < heartbeatDue) {
            {
                std::unique_lock lock(mu_);
                cv_.wait_until(lock, heartbeatDue,
                               [&] { return stopping_ || !proposals_.empty(); });
                if (stopping_) {
                    return;
                }
            }
            sendIsrProposals();
        }
        const ErrorCode error = heartbeatOnce();
        if (error == ErrorCode::STALE_BROKER_INCARNATION) {
            logging::get("broker")->warn("heartbeat_stale incarnation={} action=reregister",
                                         incarnation_);
            incarnation_ = 0;
        } else if (error != ErrorCode::NONE) {
            logging::get("broker")->warn("heartbeat_failed error={}", errorName(error));
        }
    }
}

void CoordinatorAgent::proposeIsr(IsrProposal proposal) {
    {
        std::lock_guard lock(mu_);
        proposals_.push_back(std::move(proposal));
    }
    cv_.notify_all();
}

// A proposal that could not be delivered stays queued (pendingIsr stays set) and is retried
// after a short pause (LLD §6.7).
void CoordinatorAgent::sendIsrProposals() {
    while (true) {
        std::optional<IsrProposal> proposal;
        {
            std::lock_guard lock(mu_);
            if (proposals_.empty()) {
                return;
            }
            proposal = std::move(proposals_.front());
            proposals_.pop_front();
        }
        if (!sendIsrProposal(*proposal)) {
            {
                std::lock_guard lock(mu_);
                proposals_.push_front(std::move(*proposal));
            }
            sleepFor(kInitialBackoff);
            return;
        }
    }
}

bool CoordinatorAgent::sendIsrProposal(const IsrProposal& p) {
    const auto replica = replicas_.get(p.tp);
    if (!replica) {
        return true;  // no longer hosted: nothing to change
    }
    const AlterIsrRequest request{cfg_.brokerId, incarnation_,
                                  p.tp.topic,    p.tp.partition,
                                  p.leaderEpoch, {p.newIsr.begin(), p.newIsr.end()}};
    auto frame = call(encode(request, nextCorrelationId_++));
    auto response = frame.ok() ? decodeAlterIsrResponse(frame.value())
                               : Result<AlterIsrResponse>(frame.error(), frame.message());
    if (!response.ok()) {
        logging::get("replication")->warn("alter_isr_failed error=\"{}\"", response.message());
        return false;
    }
    if (response.value().error == ErrorCode::NONE) {
        replica->onIsrCommitted(p.leaderEpoch, response.value().isr);
    } else {
        logging::get("replication")
            ->warn("alter_isr_rejected topic={} partition={} epoch={} error={}", p.tp.topic,
                   p.tp.partition, p.leaderEpoch, errorName(response.value().error));
        replica->abortIsrChange(p.leaderEpoch);
    }
    return true;
}

bool CoordinatorAgent::registerOnce() {
    auto response =
        call(encode(RegisterBrokerRequest{cfg_.brokerId, cfg_.advertisedHost, cfg_.listenPort},
                    nextCorrelationId_++));
    if (!response.ok()) {
        logging::get("broker")->warn("register_failed coordinator={} error=\"{}\"",
                                     cfg_.coordinatorAddress, response.message());
        return false;
    }
    auto registered = decodeRegisterBrokerResponse(response.value());
    if (!registered.ok() || registered.value().error != ErrorCode::NONE) {
        logging::get("broker")->warn(
            "register_rejected error={}",
            errorName(registered.ok() ? registered.value().error : registered.error()));
        return false;
    }
    incarnation_ = registered.value().incarnation;
    logging::get("broker")->info("registered coordinator={} incarnation={} advertised={}:{}",
                                 cfg_.coordinatorAddress, incarnation_, cfg_.advertisedHost,
                                 cfg_.listenPort);
    return true;
}

ErrorCode CoordinatorAgent::heartbeatOnce() {
    auto response =
        call(encode(BrokerHeartbeatRequest{cfg_.brokerId, incarnation_}, nextCorrelationId_++));
    if (!response.ok()) {
        return response.error();
    }
    auto heartbeat = decodeBrokerHeartbeatResponse(response.value());
    return heartbeat.ok() ? heartbeat.value().error : heartbeat.error();
}

// One request on the cached connection; reconnects once it has failed.
Result<Frame> CoordinatorAgent::call(const Frame& request) {
    if (!socket_) {
        auto socket = Socket::connect(coordinatorHost_, coordinatorPort_, kConnectTimeout);
        if (!socket.ok()) {
            return {socket.error(), socket.message()};
        }
        if (auto status = socket.value().setTimeouts(kRequestTimeout, kRequestTimeout);
            !status.ok()) {
            return {status.error(), status.message()};
        }
        socket_.emplace(std::move(socket).value());
    }
    auto response = writeFrame(*socket_, request).ok()
                        ? readFrame(*socket_, kMaxResponseBytes)
                        : Result<Frame>(ErrorCode::NETWORK_ERROR, "send to coordinator failed");
    if (!response.ok()) {
        socket_.reset();
        return response;
    }
    if (response.value().correlationId != request.correlationId) {
        socket_.reset();
        return {ErrorCode::CORRUPT_MESSAGE, "correlation ID mismatch"};
    }
    return response;
}

bool CoordinatorAgent::sleepFor(std::chrono::milliseconds duration) {
    std::unique_lock lock(mu_);
    return !cv_.wait_for(lock, duration, [&] { return stopping_; });
}

}  // namespace elog
