#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "elog/common/result.h"
#include "elog/common/types.h"
#include "elog/coordinator/db.h"

namespace elog {

inline constexpr BrokerId kNoLeader = -1;  // an offline partition (R-14)

struct PartitionRecord {
    std::uint32_t partition = 0;
    BrokerId leader = kNoLeader;
    Epoch leaderEpoch = 0;
    std::vector<BrokerId> replicas;  // preference order; replicas[0] is the preferred leader
    std::vector<BrokerId> isr;
};

struct TopicRecord {
    std::string name;
    std::uint32_t partitions = 0;
    std::uint32_t replicationFactor = 0;
    std::uint32_t minIsr = 0;
    std::vector<PartitionRecord> partitionStates;  // index = partition number
};

// One row of a rebalance's ownership diff (LLD §7.3): a new owner and epoch, or none to
// delete the row (the partition is unowned because the group became empty).
struct OwnershipChange {
    TopicPartition tp;
    std::optional<MemberId> owner;
    Epoch epoch = 0;
};

// One persisted group (LLD §7.6): its generation, assignor and ownership rows.
struct GroupRecord {
    std::string groupId;
    Generation generation = 0;
    std::string assignor;
    std::vector<std::pair<TopicPartition, std::pair<MemberId, Epoch>>> ownership;
};

// Every SQL statement the coordinator runs (LLD §8). Each write is one transaction, and the
// caller changes its in-memory state only after it succeeds, so memory is never ahead of
// disk (LLD §8.5).
//
// Thread rules: event-loop thread only (LLD §11.3).
class Repository {
public:
    static Result<std::unique_ptr<Repository>> open(const std::string& path);

    // The topic row with every partition_state and partition_replica row (LLD §8.5).
    Status createTopic(const TopicRecord& topic);

    Result<std::vector<TopicRecord>> loadTopics();

    // Groups, ownership and offsets (LLD §8.2, §8.4).
    Result<Txn> begin();  // for writes that span several calls, e.g. one OffsetCommit
    Status createGroup(const std::string& groupId, const std::string& assignor);
    // The new generation and the whole ownership diff in ONE transaction (LLD §8.5).
    Status saveGeneration(const std::string& groupId, Generation generation,
                          const std::vector<OwnershipChange>& diff);
    // FR-OFF-04: stores the offset only if `member` owns tp at exactly `epoch`, in one
    // statement (LLD §8.3). true: accepted; false: fenced (STALE_EPOCH).
    Result<bool> commitOffsetFenced(const std::string& groupId, const TopicPartition& tp,
                                    Offset offset, Epoch epoch, const MemberId& member,
                                    TimeMs nowMs);
    Status commitOffsetUnfenced(const std::string& groupId, const TopicPartition& tp, Offset offset,
                                Epoch epoch, TimeMs nowMs);
    Result<std::vector<GroupRecord>> loadGroups();
    // The committed offset, or nullopt if none.
    Result<std::optional<Offset>> committedOffset(const std::string& groupId,
                                                  const TopicPartition& tp);

    // Inserts or updates the broker row and returns its new incarnation (LLD §8.4).
    Result<std::uint32_t> registerBroker(BrokerId id, const std::string& host, std::uint16_t port);

private:
    explicit Repository(std::unique_ptr<Db> db) : db_(std::move(db)) {}
    Status loadPartitions(TopicRecord& topic);

    std::unique_ptr<Db> db_;
};

}  // namespace elog
