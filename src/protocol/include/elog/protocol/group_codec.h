#pragma once

#include <cstdint>

#include "elog/common/result.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/group_messages.h"

namespace elog {

// JSON codecs for the consumer-group messages (LLD §4.6–4.7), with the same rules as
// json_codec.h: decodeXxx() returns INVALID_REQUEST for a wrong API key or encoding, bad
// JSON, a missing or mistyped field, or an oversized array.

Frame encode(const JoinGroupRequest& request, std::uint32_t correlationId);
Frame encode(const JoinGroupResponse& response, std::uint32_t correlationId);
Result<JoinGroupRequest> decodeJoinGroupRequest(const Frame& frame);
Result<JoinGroupResponse> decodeJoinGroupResponse(const Frame& frame);

Frame encode(const HeartbeatRequest& request, std::uint32_t correlationId);
Frame encode(const HeartbeatResponse& response, std::uint32_t correlationId);
Result<HeartbeatRequest> decodeHeartbeatRequest(const Frame& frame);
Result<HeartbeatResponse> decodeHeartbeatResponse(const Frame& frame);

Frame encode(const LeaveGroupRequest& request, std::uint32_t correlationId);
Frame encode(const LeaveGroupResponse& response, std::uint32_t correlationId);
Result<LeaveGroupRequest> decodeLeaveGroupRequest(const Frame& frame);
Result<LeaveGroupResponse> decodeLeaveGroupResponse(const Frame& frame);

Frame encode(const OffsetCommitRequest& request, std::uint32_t correlationId);
Frame encode(const OffsetCommitResponse& response, std::uint32_t correlationId);
Result<OffsetCommitRequest> decodeOffsetCommitRequest(const Frame& frame);
Result<OffsetCommitResponse> decodeOffsetCommitResponse(const Frame& frame);

Frame encode(const OffsetFetchRequest& request, std::uint32_t correlationId);
Frame encode(const OffsetFetchResponse& response, std::uint32_t correlationId);
Result<OffsetFetchRequest> decodeOffsetFetchRequest(const Frame& frame);
Result<OffsetFetchResponse> decodeOffsetFetchResponse(const Frame& frame);

Frame encode(const DescribeGroupRequest& request, std::uint32_t correlationId);
Frame encode(const DescribeGroupResponse& response, std::uint32_t correlationId);
Result<DescribeGroupRequest> decodeDescribeGroupRequest(const Frame& frame);
Result<DescribeGroupResponse> decodeDescribeGroupResponse(const Frame& frame);

}  // namespace elog
