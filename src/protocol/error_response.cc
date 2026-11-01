#include "elog/protocol/error_response.h"

#include "elog/protocol/json_codec.h"
#include "elog/protocol/msgpack_codec.h"

namespace elog {

Frame encodeErrorResponse(const Frame& request, ErrorCode error) {
    if (request.encoding == BodyEncoding::MsgPack) {
        return encodeMsgPackErrorResponse(request.apiKey, request.correlationId, error);
    }
    return encodeJsonErrorResponse(request.apiKey, request.correlationId, error);
}

}  // namespace elog
