#include "elog/coordinator/metadata_manager.h"

#include <algorithm>
#include <utility>

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/json_codec.h"
#include "elog/protocol/replication_codec.h"

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

MetadataManager::MetadataManager(Repository& repo, EventLog& events)
    : repo_(repo), events_(events) {}

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
    created_.push_back(topic.name);
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

std::vector<PartitionState> MetadataManager::partitionsHostedBy(BrokerId broker,
                                                                const std::string* topic) const {
    std::vector<PartitionState> states;
    for (const auto& [name, t] : topics_) {
        if (topic && name != *topic) {
            continue;
        }
        for (const PartitionRecord& p : t.partitionStates) {
            if (std::find(p.replicas.begin(), p.replicas.end(), broker) != p.replicas.end()) {
                states.push_back(
                    {name, p.partition, p.leader, p.leaderEpoch, p.isr, p.replicas, t.minIsr});
            }
        }
    }
    return states;
}

// Parse → validate (current leader, current epoch, a sane ISR) → persist → apply → reply.
Frame MetadataManager::onAlterIsr(const Frame& request) {
    auto alter = decodeAlterIsrRequest(request);
    if (!alter.ok()) {
        return encodeErrorResponse(request, alter.error());
    }
    const AlterIsrRequest& r = alter.value();
    const auto reply = [&](ErrorCode error, std::vector<BrokerId> isr) {
        return encode(AlterIsrResponse{error, std::move(isr)}, request.correlationId);
    };
    const auto topic = topics_.find(r.topic);
    if (topic == topics_.end() || r.partition >= topic->second.partitionStates.size()) {
        return reply(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, {});
    }
    PartitionRecord& p = topic->second.partitionStates[r.partition];
    if (r.leaderEpoch != p.leaderEpoch || r.brokerId != p.leader) {
        return reply(ErrorCode::FENCED_LEADER_EPOCH, p.isr);  // a stale or deposed leader
    }
    const bool hasLeader = std::find(r.newIsr.begin(), r.newIsr.end(), p.leader) != r.newIsr.end();
    const bool onlyReplicas = std::all_of(r.newIsr.begin(), r.newIsr.end(), [&](BrokerId b) {
        return std::find(p.replicas.begin(), p.replicas.end(), b) != p.replicas.end();
    });
    if (!hasLeader || !onlyReplicas) {
        return reply(ErrorCode::INVALID_REQUEST, p.isr);
    }
    PartitionRecord next = p;
    next.isr = r.newIsr;
    if (auto status = repo_.savePartitionState({r.topic, r.partition}, next); !status.ok()) {
        logging::get("coordinator")
            ->critical("alter_isr_failed topic={} partition={} error=\"{}\"", r.topic, r.partition,
                       status.message());
        return reply(ErrorCode::UNKNOWN_SERVER_ERROR, p.isr);
    }
    p = std::move(next);
    std::string isr;
    for (BrokerId b : p.isr) {
        isr += (isr.empty() ? "" : ",") + std::to_string(b);
    }
    events_.emit({.event = "isr_changed",
                  .tp = TopicPartition{r.topic, r.partition},
                  .epoch = p.leaderEpoch,
                  .detail = "isr=" + isr});
    logging::get("coordinator")
        ->info("isr_changed topic={} partition={} epoch={} isr=[{}]", r.topic, r.partition,
               p.leaderEpoch, isr);
    return reply(ErrorCode::NONE, p.isr);
}

std::set<BrokerId> MetadataManager::electLeaders(
    const std::function<bool(const PartitionRecord&)>& affected,
    const std::function<bool(BrokerId)>& alive) {
    std::set<BrokerId> pushTo;
    for (auto& [name, topic] : topics_) {
        for (PartitionRecord& p : topic.partitionStates) {
            if (!affected(p)) {
                continue;
            }
            PartitionRecord next = p;
            std::erase_if(next.isr, [&](BrokerId b) { return !alive(b); });
            if (next.isr.empty()) {
                next.isr = p.isr;  // never persist an empty ISR (LLD §7.5)
            }
            if (p.leader == kNoLeader || !alive(p.leader)) {
                const auto candidate =
                    std::find_if(next.replicas.begin(), next.replicas.end(), [&](BrokerId b) {
                        return alive(b) &&
                               std::find(next.isr.begin(), next.isr.end(), b) != next.isr.end();
                    });
                next.leader = candidate == next.replicas.end() ? kNoLeader : *candidate;
                if (next.leader != kNoLeader) {
                    ++next.leaderEpoch;
                }
            }
            if (next.leader == p.leader && next.isr == p.isr && next.leaderEpoch == p.leaderEpoch) {
                continue;
            }
            if (auto status = repo_.savePartitionState({name, p.partition}, next); !status.ok()) {
                logging::get("coordinator")
                    ->critical("election_failed topic={} partition={} error=\"{}\"", name,
                               p.partition, status.message());
                continue;  // memory never ahead of disk
            }
            const bool offline = next.leader == kNoLeader;
            if (offline && p.leader != kNoLeader) {
                events_.emit(
                    {.event = "partition_offline", .tp = TopicPartition{name, p.partition}});
            } else if (!offline && next.leader != p.leader) {
                events_.emit({.event = "leader_elected",
                              .tp = TopicPartition{name, p.partition},
                              .epoch = next.leaderEpoch,
                              .detail = "leader=" + std::to_string(next.leader)});
            }
            logging::get("coordinator")
                ->info("partition_state_changed topic={} partition={} leader={} epoch={} isr={}",
                       name, p.partition, next.leader, next.leaderEpoch, next.isr.size());
            p = std::move(next);
            for (BrokerId b : p.replicas) {
                if (alive(b)) {
                    pushTo.insert(b);
                }
            }
        }
    }
    return pushTo;
}

std::uint32_t MetadataManager::partitionCount(const std::string& topic) const {
    const auto it = topics_.find(topic);
    return it == topics_.end() ? 0 : it->second.partitions;
}

std::vector<std::string> MetadataManager::takeCreatedTopics() {
    return std::exchange(created_, {});
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
