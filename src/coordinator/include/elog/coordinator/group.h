#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "elog/common/types.h"
#include "elog/coordinator/reply_slot.h"

namespace elog {

// HLD §10.1 group states.
enum class GroupState {
    EMPTY,                // no members
    PREPARING_REBALANCE,  // waiting for every member to (re)join
    STABLE,               // every member has its assignment
};

std::string_view groupStateName(GroupState state);  // "Empty", "PreparingRebalance", "Stable"

struct Member {
    MemberId id;
    std::string clientId;
    std::vector<std::string> topics;
    std::uint32_t sessionTimeoutMs = 0;
    std::uint32_t rebalanceTimeoutMs = 0;
    TimeMs lastHeartbeatMs = 0;              // monotonic
    bool rejoined = false;                   // has joined since the current rebalance started
    std::shared_ptr<ReplySlot> pendingJoin;  // a JoinGroup waiting for the rebalance to end
    std::uint32_t pendingJoinCorrelationId = 0;
};

// Who owns a partition, and since which generation (the ownership epoch).
struct Ownership {
    MemberId owner;
    Epoch epoch = 0;
};

// One consumer group (LLD §7.1). Owned by GroupManager on the event-loop thread.
struct Group {
    std::string id;
    GroupState state = GroupState::EMPTY;
    Generation generation = 0;
    std::string assignor;
    std::map<MemberId, Member> members;
    std::map<TopicPartition, Ownership> ownership;
    TimeMs rebalanceStartMs = 0;
    TimeMs rebalanceDeadline = 0;
};

}  // namespace elog
