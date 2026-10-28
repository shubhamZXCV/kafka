#pragma once

#include <cstdint>

namespace elog {

// Error codes carried in every response body (LLD §4.4). The enum lives in common so that
// Result and Socket can use it (LLD R-9); names and retriability are in protocol/error_codes.h.
enum class ErrorCode : std::int16_t {
    NETWORK_ERROR = -1,  // local only, never sent: connect failed, peer closed, socket timeout
    NONE = 0,
    CORRUPT_MESSAGE = 1,
    UNSUPPORTED_VERSION = 2,
    INVALID_REQUEST = 3,
    UNKNOWN_TOPIC_OR_PARTITION = 4,
    NOT_LEADER = 5,
    STALE_EPOCH = 6,  // only for fenced offset commits (FR-OFF-03)
    FENCED_LEADER_EPOCH = 7,
    UNKNOWN_MEMBER = 8,
    ILLEGAL_GENERATION = 9,
    REBALANCE_IN_PROGRESS = 10,
    OFFSET_OUT_OF_RANGE = 11,
    NOT_ENOUGH_REPLICAS = 12,
    THROTTLED = 13,
    REQUEST_TIMED_OUT = 14,
    INVALID_PARTITION_COUNT = 15,
    STALE_BROKER_INCARNATION = 16,
    UNKNOWN_SERVER_ERROR = 99,
};

}  // namespace elog
