#include "elog/broker/replica.h"

#include <algorithm>
#include <string>
#include <utility>

#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/record_batch.h"

namespace elog {

namespace {

// LLD §6.6, R-4: copies the records in [from, out.highWatermark) from `batches` into out,
// at most `credit` of them. Whole batches pass through unchanged; only the first (fetch
// offset inside it) and the last (credit runs out inside it) are sliced, so
// Σ records ≤ credit (FR-FETCH-04).
Status appendUpToCredit(std::span<const std::byte> batches, Offset from, std::uint32_t credit,
                        FetchData& out) {
    while (!batches.empty() && credit > 0) {
        const RecordBatchHeader h = readBatchHeader(batches);
        const auto batchBytes = batches.first(batch::kRecordBatchOverhead + h.batchLength);
        batches = batches.subspan(batchBytes.size());
        const Offset lo = std::max(h.baseOffset, from);
        const Offset hi = std::min<Offset>(h.baseOffset + h.lastOffsetDelta + 1, out.highWatermark);
        if (lo >= hi) {
            continue;
        }
        const auto n = static_cast<std::uint32_t>(std::min<Offset>(hi - lo, credit));
        if (lo == h.baseOffset && n == h.recordCount) {
            out.records.insert(out.records.end(), batchBytes.begin(), batchBytes.end());
        } else {
            auto view = RecordBatchView::parse(batchBytes);
            if (!view.ok()) {
                return {view.error(), view.message()};
            }
            const auto sliced = sliceBatch(view.value(), lo, n);
            out.records.insert(out.records.end(), sliced.begin(), sliced.end());
        }
        credit -= n;
        out.recordCount += n;
    }
    return okStatus();
}

}  // namespace

Replica::Replica(TopicPartition tp, std::unique_ptr<PartitionLog> log,
                 std::ptrdiff_t maxPendingProduce, HwNotifier& hwNotifier)
    : tp_(std::move(tp)),
      log_(std::move(log)),
      pendingSlots_(maxPendingProduce),
      hwNotifier_(hwNotifier) {}

ErrorCode Replica::applyLeaderAndIsr(const PartitionState& state, BrokerId self) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
    if (state.leaderEpoch < leaderEpoch_) {
        return ErrorCode::FENCED_LEADER_EPOCH;  // a delayed push from before a failover
    }
    const Role role = state.leader == self ? Role::LEADER : Role::FOLLOWER;
    if (state.leaderEpoch == leaderEpoch_ && role == role_ && state.leader == leaderId_) {
        return ErrorCode::NONE;  // idempotent: pushes may be retried
    }
    if (role == Role::LEADER) {
        becomeLeader(state.leaderEpoch, {state.isr.begin(), state.isr.end()}, state.replicas,
                     static_cast<std::uint16_t>(state.minIsr), self);
    } else {
        becomeFollower(state.leaderEpoch, state.leader);
    }
    leaderId_ = state.leader;
    return ErrorCode::NONE;
}

// LLD §6.3: every other replica starts as a follower we know nothing about yet.
void Replica::becomeLeader(Epoch epoch, std::set<BrokerId> isr,
                           const std::vector<BrokerId>& replicas, std::uint16_t minIsr,
                           BrokerId self) {
    leaderEpoch_ = epoch;
    role_ = Role::LEADER;
    self_ = self;
    leader_ = LeaderState{std::move(isr), std::nullopt, {}, minIsr};
    for (BrokerId replica : replicas) {
        if (replica != self) {
            leader_->followers[replica] = FollowerProgress{0, 0, 0, time::monotonicMs()};
        }
    }
    maybeAdvanceHw();
    hwCv_.notify_all();  // LLD §6.3: produces parked under the old epoch fail with NOT_LEADER
    hwNotifier_.bump();  // and long-polling fetches re-check the role
    logging::get("broker")->info("became_leader topic={} partition={} epoch={} leo={}", tp_.topic,
                                 tp_.partition, epoch, log_->leo());
}

// LLD §6.3, leader-only for now: a follower serves nothing (produce and fetch get
// NOT_LEADER); fetching from the leader comes with replication.
void Replica::becomeFollower(Epoch epoch, BrokerId leader) {
    leaderEpoch_ = epoch;
    role_ = Role::FOLLOWER;
    followerState_ = FollowerState::TRUNCATING;  // agree with the new leader before fetching
    leader_.reset();
    hwCv_.notify_all();  // produces parked under the old role fail with NOT_LEADER
    hwNotifier_.bump();
    logging::get("broker")->info("became_follower topic={} partition={} epoch={} leader={}",
                                 tp_.topic, tp_.partition, epoch, leader);
}

std::optional<Replica::ProduceSlot> Replica::tryAdmitProduce(std::chrono::milliseconds wait) {
    if (!pendingSlots_.try_acquire_for(wait)) {
        return std::nullopt;
    }
    return ProduceSlot(*this);
}

Result<AppendInfo> Replica::appendAsLeader(std::span<std::byte> batch, Epoch requestEpoch) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ → PartitionLog (LLD §11.2)
    if (auto status = checkLeaderEpoch(requestEpoch); !status.ok()) {
        return {status.error(), status.message()};
    }
    if (leader_->isr.size() < leader_->minIsr) {  // ADR-03: refuse rather than under-replicate
        return {ErrorCode::NOT_ENOUGH_REPLICAS, "isr size " + std::to_string(leader_->isr.size()) +
                                                    " below min.isr " +
                                                    std::to_string(leader_->minIsr)};
    }
    auto info = log_->append(batch, leaderEpoch_, /*assignOffsets=*/true);
    if (info.ok()) {
        maybeAdvanceHw();
        hwNotifier_.bump();  // followers long-poll for new LEO, not only for HW (LLD §6.7)
    }
    return info;
}

Result<FetchData> Replica::readForConsumer(Offset from, std::uint32_t credit, std::size_t maxBytes,
                                           Epoch requestEpoch) {
    {
        std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
        if (auto status = checkLeaderEpoch(requestEpoch); !status.ok()) {
            return {status.error(), status.message()};
        }
    }
    // The log read is lock-free; reading only below hw keeps it committed-only (INV-07).
    FetchData out{hw(), log_->logStartOffset(), {}, 0};
    if (from < out.logStartOffset || from > out.highWatermark) {
        return {ErrorCode::OFFSET_OUT_OF_RANGE, "offset " + std::to_string(from) + " outside [" +
                                                    std::to_string(out.logStartOffset) + ", " +
                                                    std::to_string(out.highWatermark) + "]"};
    }
    if (credit == 0 || maxBytes == 0 || from == out.highWatermark) {
        return out;
    }
    auto raw = log_->read(from, out.highWatermark, maxBytes);
    if (!raw.ok()) {
        return {raw.error(), raw.message()};
    }
    if (auto status = appendUpToCredit(raw.value().batches, from, credit, out); !status.ok()) {
        return {status.error(), status.message()};
    }
    return out;
}

std::size_t Replica::applyRetention(TimeMs nowWallMs) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ → PartitionLog (LLD §11.2)
    return log_->applyRetention(nowWallMs);
}

Result<ReplicaFetchData> Replica::readForFollower(BrokerId follower, Offset fetchOffset,
                                                  std::size_t maxBytes, Epoch requestEpoch,
                                                  TimeMs now) {
    ReplicaFetchData out;
    {
        std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
        if (role_ != Role::LEADER || requestEpoch != leaderEpoch_) {
            return {ErrorCode::FENCED_LEADER_EPOCH,
                    "not the leader at epoch " + std::to_string(requestEpoch)};
        }
        const auto it = leader_->followers.find(follower);
        if (it == leader_->followers.end()) {
            return {ErrorCode::INVALID_REQUEST,
                    "broker " + std::to_string(follower) + " is not a replica"};
        }
        out.leaderLeo = log_->leo();
        if (fetchOffset > out.leaderLeo || fetchOffset < log_->logStartOffset()) {
            return {ErrorCode::OFFSET_OUT_OF_RANGE, "follower must truncate"};
        }
        // LLD §6.7: caught up if it now has what we had at its previous fetch, or all of it.
        FollowerProgress& f = it->second;
        if (fetchOffset >= f.leaderLeoAtLastFetch) {
            f.lastCaughtUpMs = f.lastFetchMs;
        }
        if (fetchOffset >= out.leaderLeo) {
            f.lastCaughtUpMs = now;
        }
        f.leo = fetchOffset;
        f.lastFetchMs = now;
        f.leaderLeoAtLastFetch = out.leaderLeo;
        maybeAdvanceHw();
        out.highWatermark = hw();
    }
    // The read is lock-free; followers read up to our LEO, whole batches (LLD §5.4).
    if (fetchOffset < out.leaderLeo) {
        auto raw = log_->read(fetchOffset, out.leaderLeo, maxBytes);
        if (!raw.ok()) {
            return {raw.error(), raw.message()};
        }
        out.records = std::move(raw.value().batches);
    }
    return out;
}

std::optional<FollowerView> Replica::followerView() const {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
    if (role_ != Role::FOLLOWER || leaderId_ < 0) {
        return std::nullopt;
    }
    return FollowerView{leaderId_, leaderEpoch_, log_->leo(), followerState_};
}

void Replica::setFollowerState(Epoch epoch, FollowerState state) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
    if (role_ == Role::FOLLOWER && leaderEpoch_ == epoch) {
        followerState_ = state;
    }
}

Status Replica::appendAsFollower(std::span<std::byte> batches, Offset leaderHw, Epoch epoch) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ → PartitionLog (LLD §11.2)
    if (role_ != Role::FOLLOWER || leaderEpoch_ != epoch) {
        return okStatus();  // the role changed while the fetch was in flight
    }
    while (batches.size() >= batch::kHeaderBytes) {
        const std::size_t size = batch::kRecordBatchOverhead + readBatchHeader(batches).batchLength;
        if (size > batches.size()) {
            return {ErrorCode::CORRUPT_MESSAGE, "truncated batch from the leader"};
        }
        auto info = log_->append(batches.first(size), epoch, /*assignOffsets=*/false);
        if (!info.ok()) {
            return {info.error(), info.message()};
        }
        batches = batches.subspan(size);
    }
    // LLD §6.8: never above what the leader has committed, nor above what we hold.
    const Offset newHw = std::min(leaderHw, log_->leo());
    if (newHw != hw_.load(std::memory_order_relaxed)) {
        hw_.store(newHw, std::memory_order_release);
    }
    return okStatus();
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

// LLD §6.5: an older epoch means the client's metadata is stale; a newer one means this
// broker has not yet heard of the leadership change.
Status Replica::checkLeaderEpoch(Epoch requestEpoch) const {
    if (role_ != Role::LEADER) {
        return {ErrorCode::NOT_LEADER, "not the leader"};
    }
    if (requestEpoch < leaderEpoch_) {
        return {ErrorCode::NOT_LEADER, "request epoch " + std::to_string(requestEpoch) +
                                           " is older than " + std::to_string(leaderEpoch_)};
    }
    if (requestEpoch > leaderEpoch_) {
        return {ErrorCode::FENCED_LEADER_EPOCH, "request epoch " + std::to_string(requestEpoch) +
                                                    " is newer than " +
                                                    std::to_string(leaderEpoch_)};
    }
    return okStatus();
}

// LLD §6.4, ADR-03, ADR-13: HW = min LEO over ISR ∪ pending ISR. Counting the pending ISR
// while an AlterIsr is in flight means a shrink cannot advance HW past the follower being
// removed until the coordinator has persisted the shrink (R-7); below minISR nothing
// commits at all.
void Replica::maybeAdvanceHw() {
    if (!leader_ || leader_->isr.size() < leader_->minIsr) {
        return;
    }
    std::set<BrokerId> effective = leader_->isr;
    if (leader_->pendingIsr) {
        effective.insert(leader_->pendingIsr->begin(), leader_->pendingIsr->end());
    }
    Offset newHw = log_->leo();
    for (BrokerId replica : effective) {
        if (replica == self_) {
            continue;
        }
        const auto f = leader_->followers.find(replica);
        newHw = std::min(newHw, f == leader_->followers.end() ? Offset{0} : f->second.leo);
    }
    if (newHw > hw_.load(std::memory_order_relaxed)) {
        hw_.store(newHw, std::memory_order_release);
        hwCv_.notify_all();
        hwNotifier_.bump();
    }
}

}  // namespace elog
