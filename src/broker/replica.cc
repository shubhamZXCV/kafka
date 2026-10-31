#include "elog/broker/replica.h"

#include <string>
#include <utility>

#include "elog/common/logging.h"

namespace elog {

Replica::Replica(TopicPartition tp, std::unique_ptr<PartitionLog> log)
    : tp_(std::move(tp)), log_(std::move(log)) {}

void Replica::becomeLeader(Epoch epoch, std::set<BrokerId> isr) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
    leaderEpoch_ = epoch;
    role_ = Role::LEADER;
    leader_ = LeaderState{std::move(isr)};
    maybeAdvanceHw();
    logging::get("broker")->info("became_leader topic={} partition={} epoch={} leo={}", tp_.topic,
                                 tp_.partition, epoch, log_->leo());
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
    auto info = log_->append(batch, leaderEpoch_, /*assignOffsets=*/true);
    if (info.ok()) {
        maybeAdvanceHw();
    }
    return info;
}

// LLD §6.4 with one replica: the ISR is only this broker, so min(LEO over ISR) is our own
// log end offset. Follower LEOs join the minimum with replication.
void Replica::maybeAdvanceHw() {
    const Offset newHw = log_->leo();
    if (newHw > hw_.load(std::memory_order_relaxed)) {
        hw_.store(newHw, std::memory_order_release);
    }
}

}  // namespace elog
