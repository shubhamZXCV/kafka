#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "elog/client/rpc_connection.h"
#include "elog/common/result.h"
#include "elog/common/types.h"
#include "elog/protocol/messages.h"

namespace elog {

// Where each partition's leader is, as last reported by the coordinator's Metadata.
// Callers refresh it at start and after NOT_LEADER, FENCED_LEADER_EPOCH or a broker
// connection failure (LLD §10.3).
//
// Thread rules: every method is safe from any thread. refresh() does network I/O under
// refreshMu_ only; mu_ guards the maps and is never held across I/O.
class MetadataCache {
public:
    explicit MetadataCache(std::string coordinatorAddress);

    struct Leader {
        BrokerAddress broker;
        Epoch leaderEpoch = 0;
    };

    // Fetches metadata for `topics` (all topics if empty) and replaces what is cached for
    // them. A topic the coordinator does not know is an UNKNOWN_TOPIC_OR_PARTITION error.
    Status refresh(const std::vector<std::string>& topics);

    std::optional<Leader> leaderFor(const TopicPartition& tp) const;
    std::optional<std::uint32_t> partitionCount(const std::string& topic) const;

private:
    struct PartitionLeader {
        BrokerId leader = -1;  // -1: offline
        Epoch leaderEpoch = 0;
    };

    const std::string coordinatorAddress_;
    std::mutex refreshMu_;                      // one refresh at a time
    std::optional<RpcConnection> coordinator_;  // guarded by refreshMu_

    mutable std::mutex mu_;
    std::map<BrokerId, BrokerAddress> brokers_;
    std::map<std::string, std::vector<PartitionLeader>> topics_;
};

}  // namespace elog
