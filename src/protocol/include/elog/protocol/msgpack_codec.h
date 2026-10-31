#pragma once

#include <cstdint>

#include "elog/common/result.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/messages.h"

// MessagePack codecs for the data-plane messages (LLD §4.6–4.7). Bodies are maps with the
// same field names as the JSON messages; records travel as bin and are not parsed here
// (the broker validates batches with RecordBatchView).
//
// encode() builds a frame with a MessagePack body; responses get the response flag set.
// decodeXxx() returns INVALID_REQUEST if the frame has the wrong API key or encoding, the
// body is not one complete MessagePack value, a field is missing or has the wrong type, a
// value does not fit its field, or an array has more than kMaxRequestEntries.
namespace elog {

Frame encode(const ProduceRequest& request, std::uint32_t correlationId);
Frame encode(const ProduceResponse& response, std::uint32_t correlationId);
Result<ProduceRequest> decodeProduceRequest(const Frame& frame);
Result<ProduceResponse> decodeProduceResponse(const Frame& frame);

Frame encode(const FetchRequest& request, std::uint32_t correlationId);
Frame encode(const FetchResponse& response, std::uint32_t correlationId);
Result<FetchRequest> decodeFetchRequest(const Frame& frame);
Result<FetchResponse> decodeFetchResponse(const Frame& frame);

}  // namespace elog
