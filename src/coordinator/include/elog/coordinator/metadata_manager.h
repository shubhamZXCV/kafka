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

private:
    ErrorCode createTopic(const CreateTopicRequest& request,
                          const std::vector<BrokerAddress>& liveBrokers);
    TopicMetadata describe(const std::string& name) const;

    Repository& repo_;
    std::map<std::string, TopicRecord> topics_;
};

}  // namespace elog
