#pragma once

#include <map>
#include <string>
#include <vector>

#include "elog/coordinator/repository.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/messages.h"

namespace elog {

// Topics and partition placement (LLD §7.1, §7.5): CreateTopic and Metadata. The live
// brokers are passed in by the event loop, which owns broker liveness.
//
// Thread rules: event-loop thread only (LLD §11.3).
class MetadataManager {
public:
    explicit MetadataManager(Repository& repo);

    // Reads the persisted topics (LLD §7.6). Called once, before the loop starts.
    Status load();

    Frame onCreateTopic(const Frame& request, const std::vector<BrokerAddress>& liveBrokers);
    Frame onMetadata(const Frame& request, const std::vector<BrokerAddress>& liveBrokers) const;

    // The partitions `broker` is a replica of, as LeaderAndIsr states; only those of
    // `topic` if it is set.
    std::vector<PartitionState> partitionsHostedBy(BrokerId broker,
                                                   const std::string* topic = nullptr) const;

    // The number of partitions of `topic`, or 0 if it does not exist.
    std::uint32_t partitionCount(const std::string& topic) const;

    // Topics created since the last call, whose roles must be pushed (R-15).
    std::vector<std::string> takeCreatedTopics();

private:
    ErrorCode createTopic(const CreateTopicRequest& request,
                          const std::vector<BrokerAddress>& liveBrokers);
    TopicMetadata describe(const std::string& name) const;

    Repository& repo_;
    std::map<std::string, TopicRecord> topics_;
    std::vector<std::string> created_;
};

}  // namespace elog
