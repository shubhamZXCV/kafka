#include "elog/broker/request_dispatcher.h"

#include "elog/common/logging.h"
#include "elog/protocol/json_codec.h"
#include "elog/protocol/msgpack_codec.h"

namespace elog {

namespace {

// Answers in the request's own encoding, so the client can decode the reply (LLD R-10).
Frame errorResponse(const Frame& request, ErrorCode error) {
    if (request.encoding == BodyEncoding::MsgPack) {
        return encodeMsgPackErrorResponse(request.apiKey, request.correlationId, error);
    }
    return encodeJsonErrorResponse(request.apiKey, request.correlationId, error);
}

}  // namespace

Frame RequestDispatcher::dispatch(const Frame& request) {
    // No handlers yet: every API key is unhandled for now.
    logging::get("broker")->warn("unhandled_request api={} correlation={}",
                                 apiKeyName(request.apiKey), request.correlationId);
    return errorResponse(request, ErrorCode::INVALID_REQUEST);
}

}  // namespace elog
