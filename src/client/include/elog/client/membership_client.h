#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "elog/client/consumer.h"
#include "elog/client/rpc_connection.h"
#include "elog/common/result.h"
#include "elog/protocol/group_messages.h"

namespace elog {

// The consumer's group membership (LLD §10.4, §10.7): JoinGroup, LeaveGroup and a heartbeat
// thread. The heartbeat thread never changes the assignment; it only raises flags that the
// processing thread acts on.
//
// Thread rules: join(), leave() and resetMemberId() are called by the processing thread;
// the heartbeat thread owns heartbeatConnection_. mu_ guards memberId_, generation_ and
// joined_, and is never held across I/O.
class MembershipClient {
public:
    explicit MembershipClient(const ConsumerConfig& cfg);
    ~MembershipClient();

    // Joins (or rejoins with the current member ID) and waits for the rebalance to complete.
    // Transport errors and superseded joins are retried with backoff; UNKNOWN_MEMBER starts
    // over as a new member. Fails on INVALID_REQUEST or once stop() was called.
    Result<JoinGroupResponse> join(const std::vector<PartitionEpoch>& owned);
    void leave();
    void resetMemberId();  // the next join is a new member's
    MemberId memberId() const;

    void startHeartbeats();
    void stop();

    // Raised by the heartbeat thread (or a commit reply), cleared by the processing thread.
    std::atomic<bool> rebalanceRequested{false};
    std::atomic<bool> unknownMember{false};
    std::atomic<bool> fenced{false};  // a commit got STALE_EPOCH (FR-OFF-11)

private:
    void heartbeatLoop();
    bool sleepFor(std::chrono::milliseconds duration);  // false once stopping

    const ConsumerConfig& cfg_;
    RpcConnection joinConnection_;
    RpcConnection heartbeatConnection_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    MemberId memberId_;
    Generation generation_ = 0;
    bool joined_ = false;  // heartbeats are sent only between joins
    bool stopping_ = false;
    std::thread heartbeatThread_;
};

}  // namespace elog
