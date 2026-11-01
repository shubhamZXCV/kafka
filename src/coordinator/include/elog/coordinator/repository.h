#pragma once

#include <cstdint>
#include <memory>
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

private:
    explicit Repository(std::unique_ptr<Db> db) : db_(std::move(db)) {}
    Status loadPartitions(TopicRecord& topic);

    std::unique_ptr<Db> db_;
};

}  // namespace elog
