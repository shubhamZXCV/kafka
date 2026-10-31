#pragma once

#include <cstdint>

#include "elog/common/result.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/messages.h"

// JSON codecs for the control-plane messages (LLD §4.6–4.7).
//
// encode() builds a frame with a JSON body; responses get the response flag set.
// decodeXxx() returns INVALID_REQUEST if the frame has the wrong API key or encoding, the
// body is not valid JSON, a field is missing or has the wrong type, a value does not fit its
// field (e.g. negative in an unsigned field), or an array has more than kMaxRequestEntries.
// Unknown extra fields are ignored.
namespace elog {

Frame encode(const MetadataRequest& request, std::uint32_t correlationId);
Frame encode(const MetadataResponse& response, std::uint32_t correlationId);
Result<MetadataRequest> decodeMetadataRequest(const Frame& frame);
Result<MetadataResponse> decodeMetadataResponse(const Frame& frame);

Frame encode(const CreateTopicRequest& request, std::uint32_t correlationId);
Frame encode(const CreateTopicResponse& response, std::uint32_t correlationId);
Result<CreateTopicRequest> decodeCreateTopicRequest(const Frame& frame);
Result<CreateTopicResponse> decodeCreateTopicResponse(const Frame& frame);

Frame encode(const RegisterBrokerRequest& request, std::uint32_t correlationId);
Frame encode(const RegisterBrokerResponse& response, std::uint32_t correlationId);
Result<RegisterBrokerRequest> decodeRegisterBrokerRequest(const Frame& frame);
Result<RegisterBrokerResponse> decodeRegisterBrokerResponse(const Frame& frame);

Frame encode(const BrokerHeartbeatRequest& request, std::uint32_t correlationId);
Frame encode(const BrokerHeartbeatResponse& response, std::uint32_t correlationId);
Result<BrokerHeartbeatRequest> decodeBrokerHeartbeatRequest(const Frame& frame);
Result<BrokerHeartbeatResponse> decodeBrokerHeartbeatResponse(const Frame& frame);

Frame encode(const LeaderAndIsrRequest& request, std::uint32_t correlationId);
Frame encode(const LeaderAndIsrResponse& response, std::uint32_t correlationId);
Result<LeaderAndIsrRequest> decodeLeaderAndIsrRequest(const Frame& frame);
Result<LeaderAndIsrResponse> decodeLeaderAndIsrResponse(const Frame& frame);

}  // namespace elog
