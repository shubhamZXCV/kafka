#pragma once

#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "elog/broker/broker_config.h"
#include "elog/broker/replica_manager.h"
#include "elog/common/socket.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/messages.h"

namespace elog {

// Copies the partitions this broker follows from one leader (LLD §6.8): one ReplicaFetch for
// all of them per round, each at its own LEO, appended as a follower.
//
// Thread rules: setPartitions() and stop() are called by ReplicaFetcherManager; everything
// else runs on the fetcher thread, which owns socket_. mu_ guards partitions_ and stopping_.
class ReplicaFetcherThread {
public:
    ReplicaFetcherThread(BrokerAddress source, ReplicaManager& replicas, const BrokerConfig& cfg);
    ~ReplicaFetcherThread();

    void setPartitions(std::vector<TopicPartition> partitions);
    void stop();
    const BrokerAddress& source() const { return source_; }

private:
    void run();
    void fetchOnce();
    Result<Frame> call(const Frame& request);
    bool sleepFor(std::chrono::milliseconds duration);  // false once stopping

    const BrokerAddress source_;
    ReplicaManager& replicas_;
    const BrokerConfig& cfg_;
    std::optional<Socket> socket_;
    std::uint32_t nextCorrelationId_ = 1;

    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<TopicPartition> partitions_;
    bool stopping_ = false;
    std::thread thread_;  // last: starts after everything above
};

// One fetcher thread per leader this broker follows (LLD §6.1): reconcile() after every
// LeaderAndIsr starts, retargets and stops them.
//
// Thread rules: reconcile() and stopAll() from any thread; mu_ serialises them.
class ReplicaFetcherManager {
public:
    ReplicaFetcherManager(ReplicaManager& replicas, const BrokerConfig& cfg);

    void reconcile(const std::vector<BrokerAddress>& brokers);
    void stopAll();

private:
    ReplicaManager& replicas_;
    const BrokerConfig& cfg_;
    std::mutex mu_;
    std::map<BrokerId, BrokerAddress> addresses_;  // the latest address of every broker
    std::map<BrokerId, std::unique_ptr<ReplicaFetcherThread>> threads_;
};

}  // namespace elog
