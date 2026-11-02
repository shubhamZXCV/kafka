#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <thread>

#include "elog/broker/broker_config.h"
#include "elog/broker/replica_manager.h"
#include "elog/common/result.h"
#include "elog/common/socket.h"
#include "elog/protocol/frame.h"

namespace elog {

// The broker's link to the coordinator (LLD §6.9): registers on start, retrying with
// exponential backoff, then heartbeats every broker.heartbeat.interval.ms and registers
// again on STALE_BROKER_INCARNATION. Roles arrive separately, as LeaderAndIsr requests the
// coordinator sends to the broker's own listener. ISR proposals are queued by any thread
// and sent from the agent thread as AlterIsr, so nothing is sent under a partition lock
// (LLD §11.2).
//
// Thread rules: start() and stop() are called by the server thread, proposeIsr() by any
// thread; everything else runs on the agent thread.
class CoordinatorAgent {
public:
    CoordinatorAgent(const BrokerConfig& cfg, ReplicaManager& replicas);

    void start();
    void stop();  // wakes the agent and joins it

    // Queues an ISR change for the agent thread to send (LLD §6.7, C086).
    void proposeIsr(IsrProposal proposal);

private:
    void run();
    bool registerOnce();
    void sendIsrProposals();
    bool sendIsrProposal(const IsrProposal& proposal);  // false: retry later
    ErrorCode heartbeatOnce();
    Result<Frame> call(const Frame& request);
    bool sleepFor(std::chrono::milliseconds duration);  // false once stopping

    const BrokerConfig& cfg_;
    ReplicaManager& replicas_;
    std::string coordinatorHost_;
    std::uint16_t coordinatorPort_ = 0;
    std::optional<Socket> socket_;
    std::uint32_t incarnation_ = 0;  // 0 = not registered
    std::uint32_t nextCorrelationId_ = 1;

    std::mutex mu_;  // guards stopping_ and proposals_
    std::deque<IsrProposal> proposals_;
    std::condition_variable cv_;
    bool stopping_ = false;
    std::thread thread_;
};

}  // namespace elog
