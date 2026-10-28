#include "elog/protocol/error_codes.h"

namespace elog {

std::string_view errorName(ErrorCode code) {
    switch (code) {
        case ErrorCode::NETWORK_ERROR:
            return "NETWORK_ERROR";
        case ErrorCode::NONE:
            return "NONE";
        case ErrorCode::CORRUPT_MESSAGE:
            return "CORRUPT_MESSAGE";
        case ErrorCode::UNSUPPORTED_VERSION:
            return "UNSUPPORTED_VERSION";
        case ErrorCode::INVALID_REQUEST:
            return "INVALID_REQUEST";
        case ErrorCode::UNKNOWN_TOPIC_OR_PARTITION:
            return "UNKNOWN_TOPIC_OR_PARTITION";
        case ErrorCode::NOT_LEADER:
            return "NOT_LEADER";
        case ErrorCode::STALE_EPOCH:
            return "STALE_EPOCH";
        case ErrorCode::FENCED_LEADER_EPOCH:
            return "FENCED_LEADER_EPOCH";
        case ErrorCode::UNKNOWN_MEMBER:
            return "UNKNOWN_MEMBER";
        case ErrorCode::ILLEGAL_GENERATION:
            return "ILLEGAL_GENERATION";
        case ErrorCode::REBALANCE_IN_PROGRESS:
            return "REBALANCE_IN_PROGRESS";
        case ErrorCode::OFFSET_OUT_OF_RANGE:
            return "OFFSET_OUT_OF_RANGE";
        case ErrorCode::NOT_ENOUGH_REPLICAS:
            return "NOT_ENOUGH_REPLICAS";
        case ErrorCode::THROTTLED:
            return "THROTTLED";
        case ErrorCode::REQUEST_TIMED_OUT:
            return "REQUEST_TIMED_OUT";
        case ErrorCode::INVALID_PARTITION_COUNT:
            return "INVALID_PARTITION_COUNT";
        case ErrorCode::STALE_BROKER_INCARNATION:
            return "STALE_BROKER_INCARNATION";
        case ErrorCode::UNKNOWN_SERVER_ERROR:
            return "UNKNOWN_SERVER_ERROR";
    }
    return "UNKNOWN";  // a value read off the wire need not be a valid enumerator
}

bool isRetriable(ErrorCode code) {
    switch (code) {
        case ErrorCode::NETWORK_ERROR:
        case ErrorCode::CORRUPT_MESSAGE:
        case ErrorCode::UNKNOWN_TOPIC_OR_PARTITION:
        case ErrorCode::NOT_LEADER:
        case ErrorCode::FENCED_LEADER_EPOCH:
        case ErrorCode::REBALANCE_IN_PROGRESS:
        case ErrorCode::NOT_ENOUGH_REPLICAS:
        case ErrorCode::THROTTLED:
        case ErrorCode::REQUEST_TIMED_OUT:
            return true;
        default:
            return false;
    }
}

}  // namespace elog
