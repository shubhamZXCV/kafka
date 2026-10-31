#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>

#include "elog/common/result.h"
#include "elog/common/types.h"
#include "elog/storage/partition_log.h"

namespace elog {

enum class Role {
    FOLLOWER,  // also a new replica's role: it serves nothing until made leader
    LEADER,
};

// What only the leader tracks (LLD §6.1). Follower progress and minISR come with
// replication.
struct LeaderState {
    std::set<BrokerId> isr;
};

// One partition replica hosted by this broker: its log, role, leader epoch and high
// watermark (LLD §6.1). This first version is leader-only: with one replica the ISR is just
// this broker, so the high watermark follows the log end offset.
//
// Thread rules: mu_ guards role, epoch, leader state and every log mutation (LLD §11.1).
// hw() is lock-free and safe from any thread.
class Replica {
public:
    Replica(TopicPartition tp, std::unique_ptr<PartitionLog> log);

    // Called by the connection thread handling LeaderAndIsr (LLD §6.3, §11.3).
    void becomeLeader(Epoch epoch, std::set<BrokerId> isr);

    // Appends one batch from a producer, stamping offsets and the leader epoch (LLD §6.5).
    // Fails with NOT_LEADER if this replica is not leader or the producer's epoch is older,
    // and FENCED_LEADER_EPOCH if it is newer than ours.
    Result<AppendInfo> appendAsLeader(std::span<std::byte> batch, Epoch requestEpoch);

    // Offsets below this are committed and visible to consumers (INV-07).
    Offset hw() const { return hw_.load(std::memory_order_acquire); }

    const TopicPartition& topicPartition() const { return tp_; }

private:
    void maybeAdvanceHw();  // requires mu_

    const TopicPartition tp_;
    std::mutex mu_;
    Role role_ = Role::FOLLOWER;
    Epoch leaderEpoch_ = 0;
    std::unique_ptr<PartitionLog> log_;
    // LLD §11.4: stored with release after the records below it are in the log; readers
    // load with acquire.
    std::atomic<Offset> hw_{0};
    std::optional<LeaderState> leader_;  // set while role_ is LEADER
};

}  // namespace elog
