#pragma once

#include <cstdint>

#include "elog/common/result.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/replication_messages.h"

namespace elog {

// JSON codecs for AlterIsr and OffsetsForLeaderEpoch (LLD §4.6), with the rules of
// json_codec.h. ReplicaFetch is in msgpack_codec.h.

Frame encode(const AlterIsrRequest& request, std::uint32_t correlationId);
Frame encode(const AlterIsrResponse& response, std::uint32_t correlationId);
Result<AlterIsrRequest> decodeAlterIsrRequest(const Frame& frame);
Result<AlterIsrResponse> decodeAlterIsrResponse(const Frame& frame);

Frame encode(const OffsetsForLeaderEpochRequest& request, std::uint32_t correlationId);
Frame encode(const OffsetsForLeaderEpochResponse& response, std::uint32_t correlationId);
Result<OffsetsForLeaderEpochRequest> decodeOffsetsForLeaderEpochRequest(const Frame& frame);
Result<OffsetsForLeaderEpochResponse> decodeOffsetsForLeaderEpochResponse(const Frame& frame);

}  // namespace elog
