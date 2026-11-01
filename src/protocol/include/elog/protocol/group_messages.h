#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "elog/common/error_code.h"
#include "elog/common/types.h"
#include "elog/protocol/messages.h"

namespace elog {

// Consumer-group request and response bodies (LLD §4.6, API keys 20–25). Field names match
// the wire names. Codec: group_codec.h.

struct PartitionEpoch {
    std::string topic;
    std::uint32_t partition = 0;
    Epoch epoch = 0;  // ownership epoch
};

struct JoinGroupRequest {
    std::string groupId;
    MemberId memberId;  // "" on first join
    std::string clientId;
    std::vector<std::string> topics;
    std::uint32_t sessionTimeoutMs = 0;
    std::uint32_t rebalanceTimeoutMs = 0;
    std::vector<PartitionEpoch> owned;  // informational
};
struct JoinGroupResponse {
    ErrorCode error = ErrorCode::NONE;
    MemberId memberId;
    Generation generation = 0;
    std::string assignor;
    std::vector<PartitionEpoch> assignment;
};

struct HeartbeatRequest {
    std::string groupId;
    MemberId memberId;
    Generation generation = 0;
};
struct HeartbeatResponse {
    ErrorCode error = ErrorCode::NONE;  // or REBALANCE_IN_PROGRESS, ILLEGAL_GENERATION, ...
};

struct LeaveGroupRequest {
    std::string groupId;
    MemberId memberId;
};
struct LeaveGroupResponse {
    ErrorCode error = ErrorCode::NONE;
};

struct OffsetCommitEntry {
    std::string topic;
    std::uint32_t partition = 0;
    Offset offset = 0;  // the next offset to process
    Epoch epoch = 0;    // the committer's ownership epoch
};
struct OffsetCommitRequest {
    std::string groupId;
    MemberId memberId;
    std::vector<OffsetCommitEntry> offsets;
};
struct OffsetCommitResponse {
    ErrorCode error = ErrorCode::NONE;
    std::vector<PartitionError> offsets;  // per partition, e.g. STALE_EPOCH
};

struct OffsetFetchRequest {
    std::string groupId;
    std::vector<TopicPartition> partitions;
};
struct OffsetFetchEntry {
    std::string topic;
    std::uint32_t partition = 0;
    std::int64_t offset = -1;  // -1: nothing committed
    ErrorCode error = ErrorCode::NONE;
};
struct OffsetFetchResponse {
    ErrorCode error = ErrorCode::NONE;
    std::vector<OffsetFetchEntry> offsets;
};

struct DescribeGroupRequest {
    std::string groupId;
};
struct GroupMemberInfo {
    MemberId memberId;
    std::string clientId;
};
struct GroupPartitionInfo {
    std::string topic;
    std::uint32_t partition = 0;
    MemberId owner;
    Epoch epoch = 0;
    std::int64_t committedOffset = -1;
};
struct DescribeGroupResponse {
    ErrorCode error = ErrorCode::NONE;
    std::string state;  // Empty, PreparingRebalance, Stable
    Generation generation = 0;
    std::string assignor;
    std::vector<GroupMemberInfo> members;
    std::vector<GroupPartitionInfo> partitions;
};

}  // namespace elog
