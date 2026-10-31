#include "elog/broker/replica.h"

#include <algorithm>
#include <string>
#include <utility>

#include "elog/common/logging.h"
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

void Replica::becomeLeader(Epoch epoch, std::set<BrokerId> isr, std::uint16_t minIsr) {
    std::lock_guard lock(mu_);  // lock order: Replica::mu_ (LLD §11.2)
    leaderEpoch_ = epoch;
    role_ = Role::LEADER;
    leader_ = LeaderState{std::move(isr), minIsr};
    maybeAdvanceHw();
    hwCv_.notify_all();  // LLD §6.3: produces parked under the old epoch fail with NOT_LEADER
    hwNotifier_.bump();  // and long-polling fetches re-check the role
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

// LLD §6.4 with one replica: the ISR is only this broker, so min(LEO over ISR) is our own
// log end offset. Follower LEOs join the minimum with replication.
void Replica::maybeAdvanceHw() {
    const Offset newHw = log_->leo();
    if (newHw > hw_.load(std::memory_order_relaxed)) {
        hw_.store(newHw, std::memory_order_release);
        hwCv_.notify_all();
        hwNotifier_.bump();
    }
}

}  // namespace elog
