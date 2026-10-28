#pragma once

#include <cstdint>
#include <string_view>

namespace elog {

// Request types, carried in every frame header (LLD §4.3). JSON for the control plane,
// MessagePack for the data plane (Produce, Fetch, ReplicaFetch).
enum class ApiKey : std::uint16_t {
    Metadata = 1,
    CreateTopic = 2,
    RegisterBroker = 3,
    BrokerHeartbeat = 4,
    AlterIsr = 5,
    LeaderAndIsr = 6,
    Produce = 10,
    Fetch = 11,
    ReplicaFetch = 12,
    OffsetsForLeaderEpoch = 13,
    JoinGroup = 20,
    Heartbeat = 21,
    LeaveGroup = 22,
    OffsetCommit = 23,
    OffsetFetch = 24,
    DescribeGroup = 25,
};

// The name as written in the LLD, e.g. "OffsetCommit"; "Unknown" for any other value.
std::string_view apiKeyName(ApiKey key);

}  // namespace elog
