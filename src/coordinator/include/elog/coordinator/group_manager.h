#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <string>

#include "elog/common/events.h"
#include "elog/coordinator/group.h"
#include "elog/coordinator/reply_slot.h"
#include "elog/coordinator/repository.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/group_messages.h"

namespace elog {

struct GroupConfig {
    std::string defaultAssignor;  // group.default.assignor
    std::uint32_t minSessionTimeoutMs = 0;
    std::uint32_t maxSessionTimeoutMs = 0;
};

// Consumer-group membership and rebalances (LLD §7.3). A JoinGroup is answered only when
// its rebalance completes, so onJoin keeps the reply slot instead of returning a frame.
//
// Thread rules: event-loop thread only (LLD §11.3).
class GroupManager {
public:
    GroupManager(Repository& repo, EventLog& events, GroupConfig cfg);

    void onJoin(const Frame& request, std::shared_ptr<ReplySlot> reply, TimeMs now);

    // R-20: the coordinator is stopping; answer every pending join and refuse new ones.
    void failPendingJoins();

private:
    Group* getOrCreate(const std::string& groupId);
    void replyJoinError(Member& member, ErrorCode error);
    void prepareRebalance(Group& group, const std::string& cause, TimeMs now);
    MemberId newMemberId(const std::string& clientId);

    Repository& repo_;
    EventLog& events_;
    const GroupConfig cfg_;
    std::map<std::string, Group> groups_;
    std::mt19937_64 random_{std::random_device{}()};
    bool stopping_ = false;
};

}  // namespace elog
