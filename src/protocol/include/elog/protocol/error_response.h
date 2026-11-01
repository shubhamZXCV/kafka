#pragma once

#include "elog/common/error_code.h"
#include "elog/protocol/frame.h"

namespace elog {

// An error-only response to `request` (same API key and correlation ID), in the request's
// own encoding so the client can decode it (LLD R-10).
Frame encodeErrorResponse(const Frame& request, ErrorCode error);

}  // namespace elog
