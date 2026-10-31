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

}  // namespace elog
