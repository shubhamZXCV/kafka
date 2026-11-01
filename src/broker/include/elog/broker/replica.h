#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <set>
#include <span>
#include <utility>
#include <vector>

#include "elog/broker/hw_notifier.h"
#include "elog/common/result.h"
#include "elog/common/types.h"
#include "elog/protocol/messages.h"
#include "elog/storage/partition_log.h"

namespace elog {

enum class Role {
    FOLLOWER,  // also a new replica's role: it serves nothing until made leader
    LEADER,
};

// What only the leader tracks (LLD §6.1). Follower progress comes with replication.
struct LeaderState {
    std::set<BrokerId> isr;
    std::uint16_t minIsr = 1;  // fewer ISR members than this and nothing commits (ADR-03)
};

// Committed records for a consumer, at most the credit it asked for (LLD §6.6).
struct FetchData {
    Offset highWatermark = 0;
    Offset logStartOffset = 0;
    std::vector<std::byte> records;  // whole or sliced batches, back to back
    std::uint32_t recordCount = 0;
};

// The largest max.pending.produce allowed (LLD §12.1).
inline constexpr std::ptrdiff_t kMaxPendingProduce = 10'000;

// One partition replica hosted by this broker: its log, role, leader epoch and high
// watermark (LLD §6.1). This first version is leader-only: with one replica the ISR is just
// this broker, so the high watermark follows the log end offset.
//
// Thread rules: mu_ guards role, epoch, leader state and every log mutation (LLD §11.1).
// hw() is lock-free and safe from any thread. pendingSlots_ needs no lock.
class Replica {
public:
    using Deadline = std::chrono::steady_clock::time_point;

    // One produce admission slot, given back when this is destroyed, on every path
    // (FR-BP-04).
    class ProduceSlot {
    public:
        explicit ProduceSlot(Replica& replica) : replica_(&replica) {}
        ProduceSlot(ProduceSlot&& other) noexcept
            : replica_(std::exchange(other.replica_, nullptr)) {}
        ProduceSlot(const ProduceSlot&) = delete;
        ProduceSlot& operator=(const ProduceSlot&) = delete;
        ProduceSlot& operator=(ProduceSlot&&) = delete;
        ~ProduceSlot() {
            if (replica_) {
                replica_->pendingSlots_.release();
            }
        }

    private:
        Replica* replica_;
    };

    Replica(TopicPartition tp, std::unique_ptr<PartitionLog> log, std::ptrdiff_t maxPendingProduce,
            HwNotifier& hwNotifier);

    // Applies this partition's state from LeaderAndIsr (LLD §6.3, §11.3): an older epoch is
    // FENCED_LEADER_EPOCH, the same epoch and role is a no-op, otherwise this replica
    // becomes leader (if `self` leads) or follower.
    ErrorCode applyLeaderAndIsr(const PartitionState& state, BrokerId self);

    // Waits up to `wait` for one of max.pending.produce slots (LLD §6.5). While none is free
    // the caller's connection thread blocks, so the producer feels TCP backpressure.
    std::optional<ProduceSlot> tryAdmitProduce(std::chrono::milliseconds wait);

    // Appends one batch from a producer, stamping offsets and the leader epoch (LLD §6.5).
    // Fails with NOT_LEADER if this replica is not leader or the producer's epoch is older,
    // FENCED_LEADER_EPOCH if it is newer than ours, and NOT_ENOUGH_REPLICAS below minIsr.
    Result<AppendInfo> appendAsLeader(std::span<std::byte> batch, Epoch requestEpoch);

    // Waits until lastOffset is committed (FR-PROD-03). NOT_LEADER if leadership moved on
    // from `epoch` meanwhile, REQUEST_TIMED_OUT at the deadline.
    Status awaitCommitted(Offset lastOffset, Deadline deadline, Epoch epoch);

    // Records from `from` up to the high watermark, at most `credit` records and about
    // maxBytes (LLD §6.6). Same leader and epoch checks as appendAsLeader; OFFSET_OUT_OF_RANGE
    // outside [logStartOffset, HW].
    Result<FetchData> readForConsumer(Offset from, std::uint32_t credit, std::size_t maxBytes,
                                      Epoch requestEpoch);

    // Offsets below this are committed and visible to consumers (INV-07).
    Offset hw() const { return hw_.load(std::memory_order_acquire); }

    const TopicPartition& topicPartition() const { return tp_; }

private:
    void becomeLeader(Epoch epoch, std::set<BrokerId> isr, std::uint16_t minIsr);  // requires mu_
    void becomeFollower(Epoch epoch, BrokerId leader);                             // requires mu_
    Status checkLeaderEpoch(Epoch requestEpoch) const;                             // requires mu_
    void maybeAdvanceHw();                                                         // requires mu_

    const TopicPartition tp_;
    std::mutex mu_;
    std::condition_variable hwCv_;  // signalled on HW and role changes; waits hold mu_
    Role role_ = Role::FOLLOWER;
    Epoch leaderEpoch_ = 0;
    BrokerId leaderId_ = -1;  // who leads at leaderEpoch_ (none yet)
    std::unique_ptr<PartitionLog> log_;
    // LLD §11.4: stored with release after the records below it are in the log; readers
    // load with acquire.
    std::atomic<Offset> hw_{0};
    std::optional<LeaderState> leader_;  // set while role_ is LEADER
    std::counting_semaphore<kMaxPendingProduce> pendingSlots_;
    HwNotifier& hwNotifier_;
};

}  // namespace elog
