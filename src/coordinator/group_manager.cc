#include "elog/coordinator/group_manager.h"

#include <algorithm>
#include <cstdio>
#include <utility>

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/group_codec.h"

namespace elog {

std::string_view groupStateName(GroupState state) {
    switch (state) {
        case GroupState::EMPTY:
            return "Empty";
        case GroupState::PREPARING_REBALANCE:
            return "PreparingRebalance";
        case GroupState::STABLE:
            return "Stable";
    }
    return "?";
}

GroupManager::GroupManager(Repository& repo, EventLog& events, GroupConfig cfg)
    : repo_(repo), events_(events), cfg_(std::move(cfg)) {}

// LLD §7.3 onJoin: parse → validate → act (the reply waits for the rebalance).
void GroupManager::onJoin(const Frame& request, std::shared_ptr<ReplySlot> reply, TimeMs now) {
    auto join = decodeJoinGroupRequest(request);
    const auto refuse = [&](ErrorCode error) {
        reply->complete(encode(JoinGroupResponse{error, "", 0, "", {}}, request.correlationId));
    };
    if (!join.ok()) {
        reply->complete(encodeErrorResponse(request, join.error()));
        return;
    }
    const JoinGroupRequest& r = join.value();
    if (stopping_) {
        return refuse(ErrorCode::UNKNOWN_SERVER_ERROR);  // R-20
    }
    if (r.groupId.empty() || r.topics.empty() || r.sessionTimeoutMs < cfg_.minSessionTimeoutMs ||
        r.sessionTimeoutMs > cfg_.maxSessionTimeoutMs) {
        return refuse(ErrorCode::INVALID_REQUEST);
    }
    Group* group = getOrCreate(r.groupId);
    if (!group) {
        return refuse(ErrorCode::UNKNOWN_SERVER_ERROR);
    }
    // LLD §9.1: every member subscribes to the same topics.
    std::vector<std::string> topics = r.topics;
    std::sort(topics.begin(), topics.end());
    for (const auto& [id, other] : group->members) {
        if (id != r.memberId && other.topics != topics) {
            return refuse(ErrorCode::INVALID_REQUEST);
        }
    }
    if (!r.memberId.empty() && !group->members.contains(r.memberId)) {
        return refuse(ErrorCode::UNKNOWN_MEMBER);
    }
    const MemberId id = r.memberId.empty() ? newMemberId(r.clientId) : r.memberId;
    Member& member = group->members[id];
    member.id = id;
    member.clientId = r.clientId;
    member.topics = std::move(topics);
    member.sessionTimeoutMs = r.sessionTimeoutMs;
    member.rebalanceTimeoutMs = r.rebalanceTimeoutMs;
    member.lastHeartbeatMs = now;
    member.rejoined = true;
    if (member.pendingJoin) {
        replyJoinError(member, ErrorCode::REBALANCE_IN_PROGRESS);  // superseded duplicate
    }
    member.pendingJoin = std::move(reply);
    member.pendingJoinCorrelationId = request.correlationId;
    if (group->state != GroupState::PREPARING_REBALANCE) {
        prepareRebalance(*group, "join", now);
    }
}

void GroupManager::failPendingJoins() {
    stopping_ = true;
    for (auto& [groupId, group] : groups_) {
        for (auto& [id, member] : group.members) {
            if (member.pendingJoin) {
                replyJoinError(member, ErrorCode::UNKNOWN_SERVER_ERROR);
            }
        }
    }
}

// A group is persisted when it is created (LLD §7.3), with the default assignor.
Group* GroupManager::getOrCreate(const std::string& groupId) {
    if (auto it = groups_.find(groupId); it != groups_.end()) {
        return &it->second;
    }
    if (auto status = repo_.createGroup(groupId, cfg_.defaultAssignor); !status.ok()) {
        logging::get("coordinator")
            ->critical("create_group_failed group={} error=\"{}\"", groupId, status.message());
        return nullptr;
    }
    Group& group = groups_[groupId];
    group.id = groupId;
    group.assignor = cfg_.defaultAssignor;
    return &group;
}

void GroupManager::replyJoinError(Member& member, ErrorCode error) {
    member.pendingJoin->complete(
        encode(JoinGroupResponse{error, member.id, 0, "", {}}, member.pendingJoinCorrelationId));
    member.pendingJoin.reset();
}

// LLD §7.3: every member must rejoin before the rebalance can complete; the deadline is the
// longest rebalance timeout among them.
void GroupManager::prepareRebalance(Group& group, const std::string& cause, TimeMs now) {
    group.state = GroupState::PREPARING_REBALANCE;
    group.rebalanceStartMs = now;
    std::uint32_t timeout = 0;
    for (auto& [id, member] : group.members) {
        timeout = std::max(timeout, member.rebalanceTimeoutMs);
        if (!member.pendingJoin) {
            member.rejoined = false;
        }
    }
    group.rebalanceDeadline = now + timeout;
    events_.emit({.event = "rebalance_start",
                  .group = group.id,
                  .generation = group.generation,
                  .detail = "cause=" + cause});
    logging::get("coordinator")
        ->info("rebalance_start group={} generation={} cause={} members={}", group.id,
               group.generation, cause, group.members.size());
}

// R-20: unique across restarts without coordination.
MemberId GroupManager::newMemberId(const std::string& clientId) {
    char suffix[17];
    std::snprintf(suffix, sizeof(suffix), "%016llx", static_cast<unsigned long long>(random_()));
    return clientId + "-" + suffix;
}

}  // namespace elog
