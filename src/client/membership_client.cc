#include "elog/client/membership_client.h"

#include <algorithm>

#include "elog/common/logging.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/group_codec.h"

namespace elog {

namespace {

constexpr auto kInitialBackoff = std::chrono::milliseconds(100);
constexpr auto kMaxBackoff = std::chrono::milliseconds(2000);

std::pair<std::string, std::uint16_t> address(const std::string& coordinator) {
    auto parsed = parseHostPort(coordinator);
    return parsed.ok() ? parsed.value() : std::pair<std::string, std::uint16_t>{"", 0};
}

}  // namespace

// A join can wait for a whole rebalance, so its connection waits longer than one.
MembershipClient::MembershipClient(const ConsumerConfig& cfg)
    : cfg_(cfg),
      joinConnection_(address(cfg.coordinator).first, address(cfg.coordinator).second,
                      cfg.rebalanceTimeout + cfg.sessionTimeout + std::chrono::seconds(5)),
      heartbeatConnection_(address(cfg.coordinator).first, address(cfg.coordinator).second,
                           cfg.sessionTimeout) {}

MembershipClient::~MembershipClient() {
    stop();
}

Result<JoinGroupResponse> MembershipClient::join(const std::vector<PartitionEpoch>& owned) {
    auto backoff = kInitialBackoff;
    while (true) {
        MemberId memberId;
        {
            std::lock_guard lock(mu_);
            joined_ = false;
            memberId = memberId_;
        }
        const JoinGroupRequest request{cfg_.groupId,
                                       memberId,
                                       cfg_.clientId,
                                       cfg_.topics,
                                       static_cast<std::uint32_t>(cfg_.sessionTimeout.count()),
                                       static_cast<std::uint32_t>(cfg_.rebalanceTimeout.count()),
                                       owned};
        auto frame = joinConnection_.call(encode(request, 0));
        auto response = frame.ok() ? decodeJoinGroupResponse(frame.value())
                                   : Result<JoinGroupResponse>(frame.error(), frame.message());
        const ErrorCode error = response.ok() ? response.value().error : response.error();
        if (error == ErrorCode::NONE) {
            std::lock_guard lock(mu_);
            memberId_ = response.value().memberId;
            generation_ = response.value().generation;
            joined_ = true;
            return response;
        }
        if (error == ErrorCode::INVALID_REQUEST) {
            return {error, "join refused: invalid request"};
        }
        if (error == ErrorCode::UNKNOWN_MEMBER) {
            resetMemberId();  // evicted meanwhile: join as a new member
        }
        logging::get("consumer")
            ->warn("join_retry group={} error={}", cfg_.groupId, errorName(error));
        if (!sleepFor(backoff)) {
            return {ErrorCode::NETWORK_ERROR, "consumer stopping"};
        }
        backoff = std::min(backoff * 2, kMaxBackoff);
    }
}

void MembershipClient::leave() {
    MemberId memberId;
    {
        std::lock_guard lock(mu_);
        memberId = memberId_;
        joined_ = false;
    }
    if (!memberId.empty()) {
        joinConnection_.call(encode(LeaveGroupRequest{cfg_.groupId, memberId}, 0));  // best effort
    }
}

void MembershipClient::resetMemberId() {
    std::lock_guard lock(mu_);
    memberId_.clear();
    generation_ = 0;
    joined_ = false;
}

MemberId MembershipClient::memberId() const {
    std::lock_guard lock(mu_);
    return memberId_;
}

void MembershipClient::startHeartbeats() {
    heartbeatThread_ = std::thread([this] { heartbeatLoop(); });
}

void MembershipClient::stop() {
    {
        std::lock_guard lock(mu_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (heartbeatThread_.joinable()) {
        heartbeatThread_.join();
    }
}

// LLD §10.7. A reply is acted on only if no join started since the heartbeat was sent, so
// a heartbeat that raced a join cannot trigger another rebalance.
void MembershipClient::heartbeatLoop() {
    while (sleepFor(cfg_.heartbeatInterval)) {
        MemberId memberId;
        Generation generation = 0;
        {
            std::lock_guard lock(mu_);
            if (!joined_) {
                continue;
            }
            memberId = memberId_;
            generation = generation_;
        }
        auto frame = heartbeatConnection_.call(
            encode(HeartbeatRequest{cfg_.groupId, memberId, generation}, 0));
        auto response = frame.ok() ? decodeHeartbeatResponse(frame.value())
                                   : Result<HeartbeatResponse>(frame.error(), frame.message());
        if (!response.ok()) {
            logging::get("consumer")->warn("heartbeat_failed error=\"{}\"", response.message());
            continue;  // the coordinator may be restarting; try again next interval
        }
        std::lock_guard lock(mu_);
        if (!joined_ || generation_ != generation) {
            continue;
        }
        switch (response.value().error) {
            case ErrorCode::REBALANCE_IN_PROGRESS:
            case ErrorCode::ILLEGAL_GENERATION:
                rebalanceRequested = true;
                break;
            case ErrorCode::UNKNOWN_MEMBER:
                unknownMember = true;
                break;
            default:
                break;
        }
    }
}

bool MembershipClient::sleepFor(std::chrono::milliseconds duration) {
    std::unique_lock lock(mu_);
    return !cv_.wait_for(lock, duration, [&] { return stopping_; });
}

}  // namespace elog
