#include "elog/protocol/api_keys.h"

namespace elog {

std::string_view apiKeyName(ApiKey key) {
    switch (key) {
        case ApiKey::Metadata:
            return "Metadata";
        case ApiKey::CreateTopic:
            return "CreateTopic";
        case ApiKey::RegisterBroker:
            return "RegisterBroker";
        case ApiKey::BrokerHeartbeat:
            return "BrokerHeartbeat";
        case ApiKey::AlterIsr:
            return "AlterIsr";
        case ApiKey::LeaderAndIsr:
            return "LeaderAndIsr";
        case ApiKey::Produce:
            return "Produce";
        case ApiKey::Fetch:
            return "Fetch";
        case ApiKey::ReplicaFetch:
            return "ReplicaFetch";
        case ApiKey::OffsetsForLeaderEpoch:
            return "OffsetsForLeaderEpoch";
        case ApiKey::JoinGroup:
            return "JoinGroup";
        case ApiKey::Heartbeat:
            return "Heartbeat";
        case ApiKey::LeaveGroup:
            return "LeaveGroup";
        case ApiKey::OffsetCommit:
            return "OffsetCommit";
        case ApiKey::OffsetFetch:
            return "OffsetFetch";
        case ApiKey::DescribeGroup:
            return "DescribeGroup";
    }
    return "Unknown";  // a value read off the wire need not be a valid enumerator
}

}  // namespace elog
