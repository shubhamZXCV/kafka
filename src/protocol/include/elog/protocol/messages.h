#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "elog/common/error_code.h"
#include "elog/common/types.h"

// One struct per request and response body (LLD §4.6). Field names match the wire names.
// Codecs live in json_codec.h.
namespace elog {

// Decoders reject arrays longer than this with INVALID_REQUEST (LLD §4.7).
inline constexpr std::size_t kMaxRequestEntries = 10'000;

struct BrokerAddress {
    BrokerId id = 0;
    std::string host;
    std::uint16_t port = 0;
};

// --- Metadata (1) -------------------------------------------------------------------------

struct MetadataRequest {
    std::vector<std::string> topics;  // empty = all topics
};

struct PartitionMetadata {
    std::uint32_t partition = 0;
    BrokerId leader = 0;
    Epoch leaderEpoch = 0;
    std::vector<BrokerId> replicas;
    std::vector<BrokerId> isr;
};

struct TopicMetadata {
    std::string name;
    ErrorCode error = ErrorCode::NONE;
    std::vector<PartitionMetadata> partitions;
};

struct MetadataResponse {
    ErrorCode error = ErrorCode::NONE;
    std::vector<BrokerAddress> brokers;
    std::vector<TopicMetadata> topics;
};

// --- CreateTopic (2) ----------------------------------------------------------------------

struct CreateTopicRequest {
    std::string name;
    std::uint32_t partitions = 0;
    std::uint32_t replicationFactor = 0;
    std::uint32_t minIsr = 0;
};

struct CreateTopicResponse {
    ErrorCode error = ErrorCode::NONE;
};

// --- RegisterBroker (3) / BrokerHeartbeat (4) ---------------------------------------------

struct RegisterBrokerRequest {
    BrokerId brokerId = 0;
    std::string host;
    std::uint16_t port = 0;
};

struct RegisterBrokerResponse {
    ErrorCode error = ErrorCode::NONE;
    std::uint32_t incarnation = 0;  // bumped on every registration (LLD §7.5)
};

struct BrokerHeartbeatRequest {
    BrokerId brokerId = 0;
    std::uint32_t incarnation = 0;
};

struct BrokerHeartbeatResponse {
    ErrorCode error = ErrorCode::NONE;
};

// --- LeaderAndIsr (6) ---------------------------------------------------------------------

struct PartitionState {
    std::string topic;
    std::uint32_t partition = 0;
    BrokerId leader = 0;
    Epoch leaderEpoch = 0;
    std::vector<BrokerId> isr;
    std::vector<BrokerId> replicas;
};

struct LeaderAndIsrRequest {
    std::uint32_t controllerIncarnation = 0;
    std::vector<PartitionState> partitions;
    std::vector<BrokerAddress> brokers;  // addresses of the leaders named above
};

struct PartitionError {
    std::string topic;
    std::uint32_t partition = 0;
    ErrorCode error = ErrorCode::NONE;
};

struct LeaderAndIsrResponse {
    ErrorCode error = ErrorCode::NONE;
    std::vector<PartitionError> partitions;
};

}  // namespace elog
