#pragma once

#include <string_view>

#include "elog/common/error_code.h"

// Helpers for ErrorCode. The enum itself is in common/error_code.h (LLD R-9).
namespace elog {

// The name as written in the LLD, e.g. "STALE_EPOCH"; "UNKNOWN" for any other value.
std::string_view errorName(ErrorCode code);

// Whether a client should retry the request, possibly after reconnecting or refreshing
// metadata (the "Retriable" column of LLD §4.4). NONE is not retriable.
bool isRetriable(ErrorCode code);

}  // namespace elog
