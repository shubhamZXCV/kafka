#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "elog/client/assignment_state.h"
#include "elog/client/consumer.h"
#include "elog/client/metadata_cache.h"
#include "elog/client/rpc_connection.h"
#include "elog/common/bounded_queue.h"
#include "elog/common/metrics.h"
#include "elog/protocol/messages.h"

namespace elog {

// The consumer's fetcher thread (LLD §10.5). Before every fetch the credit is the
// in-flight queue's free slots, and with no free slot it does not fetch at all
// (FR-FETCH-05). The broker returns at most that many records, so pushing them never
// blocks.
//
// Thread rules: start() and stop() are called by the processing thread; everything else
// runs on the fetcher thread, which alone owns connections_. Positions live in the shared
// AssignmentState and are only advanced for the assignment version they were fetched under.
class Fetcher {
public:
    Fetcher(const ConsumerConfig& cfg, MetadataCache& metadata, AssignmentState& state,
            BoundedQueue<ConsumedRecord>& inflight, Metrics& metrics);

    void start();
    void stop();

private:
    void loop();
    using Owned = std::pair<TopicPartition, OwnedPartition>;
    void resolvePositions(AssignmentSnapshot& snapshot);
    void fetchFrom(const BrokerAddress& leader, const std::vector<Owned>& partitions,
                   std::uint32_t credit, bool longPoll);
    void accept(const FetchPartitionResponse& response, const Owned& owned);
    std::optional<Offset> startingOffset(const TopicPartition& tp);
    RpcConnection& connectionTo(const BrokerAddress& broker);
    void pause();  // after an error, so a failing broker is not hammered

    const ConsumerConfig& cfg_;
    MetadataCache& metadata_;
    AssignmentState& state_;
    BoundedQueue<ConsumedRecord>& inflight_;
    Metrics& metrics_;
    std::map<std::pair<std::string, std::uint16_t>, RpcConnection> connections_;
    const std::uint32_t lowWater_;      // R-17: fetch once this many slots are free
    std::uint32_t round_ = 0;           // rotates partition order for fairness
    std::size_t recordsLastRound_ = 1;  // R-17: long-poll only after an empty round
    std::atomic<bool> stopping_{false};
    std::thread thread_;
};

}  // namespace elog
