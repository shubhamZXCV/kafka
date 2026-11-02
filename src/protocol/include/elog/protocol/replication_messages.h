#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "elog/common/error_code.h"
#include "elog/common/types.h"

namespace elog {

// Replication request and response bodies (LLD §4.6). ReplicaFetch is MessagePack
// (msgpack_codec.h); AlterIsr and OffsetsForLeaderEpoch are JSON (replication_codec.h).

struct ReplicaFetchPartition {
    std::string topic;
    std::uint32_t partition = 0;
    Offset fetchOffset = 0;  // the follower's LEO: also acknowledges everything below it
    Epoch currentLeaderEpoch = 0;
};
struct ReplicaFetchRequest {
    BrokerId replicaId = 0;
    std::uint32_t maxWaitMs = 0;
    std::uint32_t maxBytes = 0;
    std::vector<ReplicaFetchPartition> partitions;
};
struct ReplicaFetchPartitionResponse {
    std::string topic;
    std::uint32_t partition = 0;
    ErrorCode error = ErrorCode::NONE;
    Offset highWatermark = 0;
    Offset leaderLeo = 0;
    std::vector<std::byte> records;  // whole batches
};
struct ReplicaFetchResponse {
    ErrorCode error = ErrorCode::NONE;
    std::vector<ReplicaFetchPartitionResponse> partitions;
};

struct EpochQuery {
    std::string topic;
    std::uint32_t partition = 0;
    Epoch leaderEpoch = 0;
};
struct OffsetsForLeaderEpochRequest {
    std::vector<EpochQuery> partitions;
};
struct EpochEndOffset {
    std::string topic;
    std::uint32_t partition = 0;
    ErrorCode error = ErrorCode::NONE;
    Epoch leaderEpoch = 0;  // the largest epoch the leader has that is <= the one asked
    Offset endOffset = 0;   // where that epoch ends on the leader
};
struct OffsetsForLeaderEpochResponse {
    ErrorCode error = ErrorCode::NONE;
    std::vector<EpochEndOffset> partitions;
};

struct AlterIsrRequest {
    BrokerId brokerId = 0;
    std::uint32_t incarnation = 0;
    std::string topic;
    std::uint32_t partition = 0;
    Epoch leaderEpoch = 0;
    std::vector<BrokerId> newIsr;
};
struct AlterIsrResponse {
    ErrorCode error = ErrorCode::NONE;  // FENCED_LEADER_EPOCH if the epoch is stale
    std::vector<BrokerId> isr;
};

}  // namespace elog
