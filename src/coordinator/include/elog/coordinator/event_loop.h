#pragma once

#include <chrono>
#include <map>
#include <string>
#include <vector>

#include "elog/common/bounded_queue.h"
#include "elog/common/events.h"
#include "elog/common/types.h"
#include "elog/coordinator/broker_liveness.h"
#include "elog/coordinator/broker_pusher.h"
#include "elog/coordinator/coordinator_config.h"
#include "elog/coordinator/event.h"
#include "elog/coordinator/group_manager.h"
#include "elog/coordinator/metadata_manager.h"
#include "elog/coordinator/offset_manager.h"
#include "elog/coordinator/repository.h"
#include "elog/protocol/frame.h"

namespace elog {

// The single thread that owns all coordinator state (LLD §7.2, ADR-02). Connection threads
// only queue requests and wait for replies, so state needs no locks: requests are
// serialised instead.
//
// Thread rules: run() is the event-loop thread; nothing else calls into this class.
class EventLoop {
public:
    EventLoop(const CoordinatorConfig& cfg, Repository& repo, BrokerPusher& pusher,
              EventLog& events);

    // Loads persisted state (LLD §7.6). Called before run().
    Status load();

    // Handles requests and ticks until `inbound` is closed and drained.
    void run(BoundedQueue<LoopEvent>& inbound);

private:
    void handle(RequestEvent& event);
    void handle(const PushResultEvent& event);
    void handle(const ShutdownEvent& event);
    Frame dispatch(const Frame& request);
    void onTick(TimeMs now);
    void pushNewRoles();
    void push(BrokerId broker, const std::string* topic);

    struct PushRetry {
        std::uint32_t incarnation = 0;
        TimeMs dueMs = 0;
    };

    const std::chrono::milliseconds tick_;
    MetadataManager meta_;
    BrokerLiveness brokers_;
    GroupManager groups_;
    OffsetManager offsets_;
    BrokerPusher& pusher_;
    const std::uint32_t controllerIncarnation_;  // start time in seconds, informational (R-15)
    std::map<BrokerId, PushRetry> pushRetries_;
};

}  // namespace elog
