#include "elog/coordinator/metadata_manager.h"

#include <algorithm>

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/json_codec.h"

namespace elog {

namespace {

constexpr std::uint32_t kMaxPartitions = 10'000;  // R-14

// R-14: the name becomes a directory on every broker, so it must stay a plain file name.
bool validTopicName(const std::string& name) {
    if (name.empty() || name.size() > 249 || name == "." || name == "..") {
        return false;
    }
    return std::all_of(name.begin(), name.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
               c == '.' || c == '_' || c == '-';
    });
}

// LLD §7.5: with live brokers sorted by ID as B[0..n), replicas(p) = B[(p + i) mod n] for
// i in [0, R); the first replica leads, at epoch 0.
TopicRecord place(const CreateTopicRequest& request, std::vector<BrokerAddress> brokers) {
    std::sort(brokers.begin(), brokers.end(),
              [](const BrokerAddress& a, const BrokerAddress& b) { return a.id < b.id; });
    TopicRecord topic{
        request.name, request.partitions, request.replicationFactor, request.minIsr, {}};
    for (std::uint32_t p = 0; p < request.partitions; ++p) {
        PartitionRecord partition{p, kNoLeader, 0, {}, {}};
        for (std::uint32_t i = 0; i < request.replicationFactor; ++i) {
            partition.replicas.push_back(brokers[(p + i) % brokers.size()].id);
        }
        partition.leader = partition.replicas.front();
        partition.isr = partition.replicas;
        topic.partitionStates.push_back(std::move(partition));
    }
    return topic;
}

}  // namespace

MetadataManager::MetadataManager(Repository& repo) : repo_(repo) {}

Status MetadataManager::load() {
    auto topics = repo_.loadTopics();
    if (!topics.ok()) {
        return {topics.error(), topics.message()};
    }
    for (TopicRecord& topic : topics.value()) {
        topics_.emplace(topic.name, std::move(topic));
    }
    logging::get("coordinator")->info("topics_loaded count={}", topics_.size());
    return okStatus();
}

Frame MetadataManager::onCreateTopic(const Frame& request,
                                     const std::vector<BrokerAddress>& liveBrokers) {
    auto create = decodeCreateTopicRequest(request);
    if (!create.ok()) {
        return encodeErrorResponse(request, create.error());
    }
    const ErrorCode error = createTopic(create.value(), liveBrokers);
    return encode(CreateTopicResponse{error}, request.correlationId);
}

// Validate → act (persist, then remember) → reply. Nothing changes in memory unless the
// transaction committed (LLD §8.5).
ErrorCode MetadataManager::createTopic(const CreateTopicRequest& request,
                                       const std::vector<BrokerAddress>& liveBrokers) {
    if (!validTopicName(request.name) || request.partitions < 1 ||
        request.partitions > kMaxPartitions || request.minIsr < 1 ||
        request.minIsr > request.replicationFactor) {
        return ErrorCode::INVALID_REQUEST;
    }
    if (auto it = topics_.find(request.name); it != topics_.end()) {  // idempotent (FR-PART-06)
        const TopicRecord& existing = it->second;
        if (existing.partitions != request.partitions) {
            return ErrorCode::INVALID_PARTITION_COUNT;
        }
        const bool same = existing.replicationFactor == request.replicationFactor &&
                          existing.minIsr == request.minIsr;
        return same ? ErrorCode::NONE : ErrorCode::INVALID_REQUEST;
    }
    if (liveBrokers.size() < request.replicationFactor) {
        return ErrorCode::NOT_ENOUGH_REPLICAS;
    }
    TopicRecord topic = place(request, liveBrokers);
    if (auto status = repo_.createTopic(topic); !status.ok()) {
        logging::get("coordinator")
            ->critical("create_topic_failed topic={} error=\"{}\"", request.name, status.message());
        return ErrorCode::UNKNOWN_SERVER_ERROR;
    }
    logging::get("coordinator")
        ->info("topic_created topic={} partitions={} replication_factor={} min_isr={}", topic.name,
               topic.partitions, topic.replicationFactor, topic.minIsr);
    topics_.emplace(topic.name, std::move(topic));
    return ErrorCode::NONE;
}

Frame MetadataManager::onMetadata(const Frame& request,
                                  const std::vector<BrokerAddress>& liveBrokers) const {
    auto metadata = decodeMetadataRequest(request);
    if (!metadata.ok()) {
        return encodeErrorResponse(request, metadata.error());
    }
    MetadataResponse response{ErrorCode::NONE, liveBrokers, {}};
    if (metadata.value().topics.empty()) {
        for (const auto& [name, topic] : topics_) {
            response.topics.push_back(describe(name));
        }
    }
    for (const std::string& name : metadata.value().topics) {
        response.topics.push_back(describe(name));
    }
    return encode(response, request.correlationId);
}

TopicMetadata MetadataManager::describe(const std::string& name) const {
    const auto it = topics_.find(name);
    if (it == topics_.end()) {
        return {name, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, {}};
    }
    TopicMetadata out{name, ErrorCode::NONE, {}};
    for (const PartitionRecord& p : it->second.partitionStates) {
        out.partitions.push_back({p.partition, p.leader, p.leaderEpoch, p.replicas, p.isr});
    }
    return out;
}

}  // namespace elog
