#include "elog/client/metadata_cache.h"

#include <utility>

#include "elog/protocol/error_codes.h"
#include "elog/protocol/json_codec.h"

namespace elog {

MetadataCache::MetadataCache(std::string coordinatorAddress)
    : coordinatorAddress_(std::move(coordinatorAddress)) {}

Status MetadataCache::refresh(const std::vector<std::string>& topics) {
    std::lock_guard refreshLock(refreshMu_);
    if (!coordinator_) {
        auto address = parseHostPort(coordinatorAddress_);
        if (!address.ok()) {
            return {address.error(), address.message()};
        }
        coordinator_.emplace(address.value().first, address.value().second);
    }
    auto frame = coordinator_->call(encode(MetadataRequest{topics}, 0));
    if (!frame.ok()) {
        return {frame.error(), "metadata from " + coordinatorAddress_ + ": " + frame.message()};
    }
    auto metadata = decodeMetadataResponse(frame.value());
    if (!metadata.ok()) {
        return {metadata.error(), metadata.message()};
    }
    if (metadata.value().error != ErrorCode::NONE) {
        return {metadata.value().error, "metadata refused"};
    }

    std::lock_guard lock(mu_);
    for (const BrokerAddress& broker : metadata.value().brokers) {
        brokers_[broker.id] = broker;
    }
    Status result = okStatus();  // the first topic error, after applying the rest
    for (const TopicMetadata& topic : metadata.value().topics) {
        if (topic.error != ErrorCode::NONE) {
            if (result.ok()) {
                result = {topic.error,
                          "topic " + topic.name + ": " + std::string(errorName(topic.error))};
            }
            continue;
        }
        std::vector<PartitionLeader> leaders(topic.partitions.size());
        for (const PartitionMetadata& p : topic.partitions) {
            if (p.partition < leaders.size()) {
                leaders[p.partition] = {p.leader, p.leaderEpoch};
            }
        }
        topics_[topic.name] = std::move(leaders);
    }
    return result;
}

std::optional<MetadataCache::Leader> MetadataCache::leaderFor(const TopicPartition& tp) const {
    std::lock_guard lock(mu_);
    const auto topic = topics_.find(tp.topic);
    if (topic == topics_.end() || tp.partition >= topic->second.size()) {
        return std::nullopt;
    }
    const PartitionLeader& p = topic->second[tp.partition];
    const auto broker = brokers_.find(p.leader);
    if (broker == brokers_.end()) {
        return std::nullopt;  // offline, or its broker is not live
    }
    return Leader{broker->second, p.leaderEpoch};
}

std::optional<std::uint32_t> MetadataCache::partitionCount(const std::string& topic) const {
    std::lock_guard lock(mu_);
    const auto it = topics_.find(topic);
    if (it == topics_.end()) {
        return std::nullopt;
    }
    return static_cast<std::uint32_t>(it->second.size());
}

}  // namespace elog
