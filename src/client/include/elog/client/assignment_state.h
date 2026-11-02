#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <vector>

#include "elog/common/types.h"
#include "elog/protocol/group_messages.h"

namespace elog {

// One partition this consumer currently owns (LLD §10.4).
struct OwnedPartition {
    Epoch epoch = 0;                      // ownership epoch, sent with commits
    std::uint64_t assignedVersion = 0;    // state version at which it was (re)assigned
    std::optional<Offset> fetchPosition;  // none: resolve with auto.offset.reset
    Offset processedNext = 0;             // one past the last processed record
    std::optional<Offset> lastCommitted;
};

struct AssignmentSnapshot {
    std::uint64_t version = 0;
    std::map<TopicPartition, OwnedPartition> owned;
};

struct AssignmentDelta {
    std::vector<TopicPartition> added, revoked, retained;
};

// What the consumer owns and how far it has fetched and processed each partition
// (LLD §10.4). The processing thread changes the assignment; the fetcher reads snapshots and
// advances fetch positions. A partition's assignedVersion changes whenever it is (re)assigned,
// so records and position updates from a fetch made under an older assignment are
// recognised and dropped; a partition retained by a sticky rebalance keeps its version.
//
// Thread rules: safe from any thread; mu_ guards everything and is never held across I/O.
class AssignmentState {
public:
    AssignmentSnapshot snapshot() const;

    // Applies a new assignment. Retained partitions (same epoch) keep their positions.
    AssignmentDelta apply(Generation generation, const std::vector<PartitionEpoch>& assignment);
    void revokeAll();

    // Sets the fetch position if the partition is still owned under assignedVersion.
    bool setFetchPosition(const TopicPartition& tp, std::uint64_t assignedVersion, Offset position);
    // Where processing (and fetching) starts after an assignment, e.g. the committed offset.
    void startAt(const TopicPartition& tp, Offset position, bool committed);

    // Whether a record fetched under assignedVersion may still be processed.
    bool owns(const TopicPartition& tp, std::uint64_t assignedVersion) const;
    void markProcessed(const TopicPartition& tp, Offset next);

    Generation generation() const;

private:
    mutable std::mutex mu_;
    std::uint64_t version_ = 0;
    Generation generation_ = 0;
    std::map<TopicPartition, OwnedPartition> owned_;
};

}  // namespace elog
