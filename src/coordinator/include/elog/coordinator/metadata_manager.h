#pragma once

#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "elog/common/events.h"
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
    MetadataManager(Repository& repo, EventLog& events);

    // Reads the persisted topics (LLD §7.6). Called once, before the loop starts.
    Status load();

    Frame onCreateTopic(const Frame& request, const std::vector<BrokerAddress>& liveBrokers);
    Frame onMetadata(const Frame& request, const std::vector<BrokerAddress>& liveBrokers) const;

    // LLD §7.5: a leader's ISR change, accepted only from the current leader at the current
    // epoch, persisted before the reply.
    Frame onAlterIsr(const Frame& request);

    // The partitions `broker` is a replica of, as LeaderAndIsr states; only those of
    // `topic` if it is set.
    std::vector<PartitionState> partitionsHostedBy(BrokerId broker,
                                                   const std::string* topic = nullptr) const;

    // LLD §7.5 ISR-only election. `affected` picks the partitions to re-examine (e.g. led
    // by, or with an ISR holding, a dead broker); for each, brokers that are not `alive`
    // leave the ISR, and if the leader is gone the first live ISR replica in preference
    // order leads at epoch + 1 (none: offline, never an unclean election, ADR-03). Each
    // change is persisted before anything is pushed. Returns the live replicas to push to.
    std::set<BrokerId> electLeaders(const std::function<bool(const PartitionRecord&)>& affected,
                                    const std::function<bool(BrokerId)>& alive);

    // The number of partitions of `topic`, or 0 if it does not exist.
    std::uint32_t partitionCount(const std::string& topic) const;

    // Topics created since the last call, whose roles must be pushed (R-15).
    std::vector<std::string> takeCreatedTopics();

private:
    ErrorCode createTopic(const CreateTopicRequest& request,
                          const std::vector<BrokerAddress>& liveBrokers);
    TopicMetadata describe(const std::string& name) const;

    Repository& repo_;
    EventLog& events_;
    std::map<std::string, TopicRecord> topics_;
    std::vector<std::string> created_;
};

}  // namespace elog
