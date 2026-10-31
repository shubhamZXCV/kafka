#include "elog/broker/replica.h"

#include <string>
#include <utility>

#include "elog/common/logging.h"

namespace elog {

Replica::Replica(TopicPartition tp, std::unique_ptr<PartitionLog> log,
                 std::ptrdiff_t maxPendingProduce)
    : tp_(std::move(tp)), log_(std::move(log)), pendingSlots_(maxPendingProduce) {}

void Replica::becomeLeader(Epoch epoch, std::set<BrokerId> isr, std::uint16_t minIsr) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
    leaderEpoch_ = epoch;
    role_ = Role::LEADER;
    leader_ = LeaderState{std::move(isr), minIsr};
    maybeAdvanceHw();
    hwCv_.notify_all();  // LLD §6.3: produces parked under the old epoch fail with NOT_LEADER
    logging::get("broker")->info("became_leader topic={} partition={} epoch={} leo={}", tp_.topic,
                                 tp_.partition, epoch, log_->leo());
}

std::optional<Replica::ProduceSlot> Replica::tryAdmitProduce(std::chrono::milliseconds wait) {
    if (!pendingSlots_.try_acquire_for(wait)) {
        return std::nullopt;
    }
    return ProduceSlot(*this);
}

Result<AppendInfo> Replica::appendAsLeader(std::span<std::byte> batch, Epoch requestEpoch) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ → PartitionLog (LLD §11.2)
    if (role_ != Role::LEADER) {
        return {ErrorCode::NOT_LEADER, "not the leader"};
    }
    // LLD §6.5: an older epoch means the producer's metadata is stale; a newer one means
    // this broker has not yet heard of the leadership change.
    if (requestEpoch < leaderEpoch_) {
        return {ErrorCode::NOT_LEADER, "producer epoch " + std::to_string(requestEpoch) +
                                           " is older than " + std::to_string(leaderEpoch_)};
    }
    if (requestEpoch > leaderEpoch_) {
        return {ErrorCode::FENCED_LEADER_EPOCH, "producer epoch " + std::to_string(requestEpoch) +
                                                    " is newer than " +
                                                    std::to_string(leaderEpoch_)};
    }
    if (leader_->isr.size() < leader_->minIsr) {  // ADR-03: refuse rather than under-replicate
        return {ErrorCode::NOT_ENOUGH_REPLICAS, "isr size " + std::to_string(leader_->isr.size()) +
                                                    " below min.isr " +
                                                    std::to_string(leader_->minIsr)};
    }
    auto info = log_->append(batch, leaderEpoch_, /*assignOffsets=*/true);
    if (info.ok()) {
        maybeAdvanceHw();
    }
    return info;
}

Status Replica::awaitCommitted(Offset lastOffset, Deadline deadline, Epoch epoch) {
    std::unique_lock lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
    const auto leadershipMoved = [&] { return role_ != Role::LEADER || leaderEpoch_ != epoch; };
    hwCv_.wait_until(lock, deadline, [&] { return hw() > lastOffset || leadershipMoved(); });
    if (hw() > lastOffset) {
        return okStatus();
    }
    if (leadershipMoved()) {
        return {ErrorCode::NOT_LEADER, "leadership moved while waiting for commit"};
    }
    return {ErrorCode::REQUEST_TIMED_OUT, "not committed before the produce timeout"};
}

// LLD §6.4 with one replica: the ISR is only this broker, so min(LEO over ISR) is our own
// log end offset. Follower LEOs join the minimum with replication.
void Replica::maybeAdvanceHw() {
    const Offset newHw = log_->leo();
    if (newHw > hw_.load(std::memory_order_relaxed)) {
        hw_.store(newHw, std::memory_order_release);
        hwCv_.notify_all();
    }
}

}  // namespace elog
