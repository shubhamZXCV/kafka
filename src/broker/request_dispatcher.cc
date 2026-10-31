#include "elog/broker/request_dispatcher.h"

#include "elog/common/logging.h"
#include "elog/protocol/json_codec.h"
#include "elog/protocol/messages.h"
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

RequestDispatcher::RequestDispatcher(ProduceHandler& produce, FetchHandler& fetch)
    : produce_(produce), fetch_(fetch) {}

Frame RequestDispatcher::dispatch(const Frame& request) {
    switch (request.apiKey) {
        case ApiKey::Produce:
            return handleProduce(request);
        case ApiKey::Fetch:
            return handleFetch(request);
        default:
            break;
    }
    logging::get("broker")->warn("unhandled_request api={} correlation={}",
                                 apiKeyName(request.apiKey), request.correlationId);
    return errorResponse(request, ErrorCode::INVALID_REQUEST);
}

// Parse → act → reply. A body that does not decode gets INVALID_REQUEST; the frame itself
// was valid, so the connection stays open (R-10).
Frame RequestDispatcher::handleProduce(const Frame& request) {
    auto produce = decodeProduceRequest(request);
    if (!produce.ok()) {
        logging::get("broker")->warn("bad_request api=Produce correlation={} error=\"{}\"",
                                     request.correlationId, produce.message());
        return errorResponse(request, produce.error());
    }
    return encode(produce_.handle(produce.value()), request.correlationId);
}

Frame RequestDispatcher::handleFetch(const Frame& request) {
    auto fetch = decodeFetchRequest(request);
    if (!fetch.ok()) {
        logging::get("broker")->warn("bad_request api=Fetch correlation={} error=\"{}\"",
                                     request.correlationId, fetch.message());
        return errorResponse(request, fetch.error());
    }
    return encode(fetch_.handle(fetch.value()), request.correlationId);
}

}  // namespace elog
