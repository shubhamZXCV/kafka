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

GroupManager::GroupManager(Repository& repo, const MetadataManager& meta, EventLog& events,
                           GroupConfig cfg)
    : repo_(repo), meta_(meta), events_(events), cfg_(std::move(cfg)) {}

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
    maybeCompleteRebalance(*group, now);
}

Frame GroupManager::onHeartbeat(const Frame& request, TimeMs now) {
    auto heartbeat = decodeHeartbeatRequest(request);
    if (!heartbeat.ok()) {
        return encodeErrorResponse(request, heartbeat.error());
    }
    Group* group = nullptr;
    Member* member = findMember(heartbeat.value().groupId, heartbeat.value().memberId, &group);
    ErrorCode error = ErrorCode::NONE;
    if (!member) {
        error = ErrorCode::UNKNOWN_MEMBER;
    } else {
        member->lastHeartbeatMs = now;
        if (group->state == GroupState::PREPARING_REBALANCE) {
            error = ErrorCode::REBALANCE_IN_PROGRESS;  // the member must rejoin
        } else if (heartbeat.value().generation != group->generation) {
            error = ErrorCode::ILLEGAL_GENERATION;
        }
    }
    return encode(HeartbeatResponse{error}, request.correlationId);
}

// FR-GRP-05: a graceful leave starts the rebalance at once.
Frame GroupManager::onLeave(const Frame& request, TimeMs now) {
    auto leave = decodeLeaveGroupRequest(request);
    if (!leave.ok()) {
        return encodeErrorResponse(request, leave.error());
    }
    Group* group = nullptr;
    if (!findMember(leave.value().groupId, leave.value().memberId, &group)) {
        return encode(LeaveGroupResponse{ErrorCode::UNKNOWN_MEMBER}, request.correlationId);
    }
    removeMember(*group, leave.value().memberId, "leave", now, true);
    return encode(LeaveGroupResponse{ErrorCode::NONE}, request.correlationId);
}

// A member waiting in JoinGroup is not evicted for missing heartbeats: the rebalance
// deadline covers it (LLD §7.3).
void GroupManager::onTick(TimeMs now) {
    for (auto& [groupId, group] : groups_) {
        std::vector<MemberId> expired;
        for (const auto& [id, member] : group.members) {
            if (!member.pendingJoin && now - member.lastHeartbeatMs > member.sessionTimeoutMs) {
                expired.push_back(id);
            }
        }
        for (const MemberId& id : expired) {
            events_.emit({.event = "member_evicted",
                          .group = groupId,
                          .member = id,
                          .detail = "cause=session_timeout"});
            removeMember(group, id, "timeout", now, true);
        }
        if (group.state == GroupState::PREPARING_REBALANCE && now >= group.rebalanceDeadline) {
            std::vector<MemberId> missing;
            for (const auto& [id, member] : group.members) {
                if (!member.rejoined) {
                    missing.push_back(id);
                }
            }
            for (const MemberId& id : missing) {
                events_.emit({.event = "member_evicted",
                              .group = groupId,
                              .member = id,
                              .detail = "cause=rebalance_timeout"});
                removeMember(group, id, "rebalance_timeout", now, false);
            }
            completeRebalance(group, now);
        }
    }
}

// LLD §7.3: ownership rows are NOT touched here; they change only in completeRebalance, so a
// removed member's commits stay fenced by the CAS until its partitions are reassigned.
void GroupManager::removeMember(Group& group, const MemberId& id, const std::string& cause,
                                TimeMs now, bool complete) {
    Member& member = group.members.at(id);
    if (member.pendingJoin) {
        replyJoinError(member, ErrorCode::UNKNOWN_MEMBER);
    }
    group.members.erase(id);
    logging::get("coordinator")
        ->info("member_removed group={} member={} cause={}", group.id, id, cause);
    if (group.state != GroupState::PREPARING_REBALANCE) {
        prepareRebalance(group, cause, now);
    }
    if (complete) {
        maybeCompleteRebalance(group, now);
    }
}

Member* GroupManager::findMember(const std::string& groupId, const MemberId& id, Group** group) {
    const auto g = groups_.find(groupId);
    if (g == groups_.end()) {
        return nullptr;
    }
    const auto m = g->second.members.find(id);
    if (m == g->second.members.end()) {
        return nullptr;
    }
    *group = &g->second;
    return &m->second;
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

// LLD §7.3: the barrier: complete once every member has rejoined.
void GroupManager::maybeCompleteRebalance(Group& group, TimeMs now) {
    if (group.state != GroupState::PREPARING_REBALANCE) {
        return;
    }
    for (const auto& [id, member] : group.members) {
        if (!member.rejoined) {
            return;
        }
    }
    completeRebalance(group, now);
}

// LLD §7.3 completeRebalance: assign, persist the generation and ownership diff in one
// transaction, and only then apply it and answer the joins (P4: never reply ahead of disk).
void GroupManager::completeRebalance(Group& group, TimeMs now) {
    const auto assignor = makeAssignor(group.assignor);
    if (!assignor) {  // only configured names are stored, so this is a bug
        logging::get("coordinator")
            ->critical("unknown_assignor group={} assignor={}", group.id, group.assignor);
        return;
    }
    const Generation generation = group.generation + 1;
    const std::vector<OwnershipChange> diff = ownershipDiff(group, *assignor, generation);
    if (auto status = repo_.saveGeneration(group.id, generation, diff); !status.ok()) {
        logging::get("coordinator")
            ->critical("save_generation_failed group={} error=\"{}\"", group.id, status.message());
        for (auto& [id, member] : group.members) {
            if (member.pendingJoin) {
                replyJoinError(member, ErrorCode::UNKNOWN_SERVER_ERROR);
            }
        }
        return;  // memory unchanged; the members join again
    }
    group.generation = generation;
    for (const OwnershipChange& change : diff) {
        if (change.owner) {
            group.ownership[change.tp] = {*change.owner, change.epoch};
        } else {
            group.ownership.erase(change.tp);
        }
    }
    group.state = group.members.empty() ? GroupState::EMPTY : GroupState::STABLE;
    replyAssignments(group, now);
    events_.emit({.event = "rebalance_end",
                  .group = group.id,
                  .generation = generation,
                  .detail = "duration_ms=" + std::to_string(now - group.rebalanceStartMs) +
                            ";moved=" + std::to_string(diff.size()) +
                            ";members=" + std::to_string(group.members.size())});
    logging::get("coordinator")
        ->info("rebalance_end group={} generation={} members={} moved={}", group.id, generation,
               group.members.size(), diff.size());
}

// LLD §7.3: a partition that changed owner, or every partition under an eager assignor,
// gets the new generation as its ownership epoch; a retained one keeps its epoch.
std::vector<OwnershipChange> GroupManager::ownershipDiff(const Group& group,
                                                         const Assignor& assignor,
                                                         Generation generation) const {
    std::vector<MemberId> members;
    std::vector<TopicPartition> partitions;
    Assignment previous;
    for (const auto& [id, member] : group.members) {
        members.push_back(id);
    }
    if (!members.empty()) {
        for (const std::string& topic : group.members.begin()->second.topics) {
            for (std::uint32_t p = 0; p < meta_.partitionCount(topic); ++p) {
                partitions.push_back({topic, p});
            }
        }
    }
    for (const auto& [tp, ownership] : group.ownership) {
        if (group.members.contains(ownership.owner)) {
            previous[tp] = ownership.owner;
        }
    }
    const Assignment next =
        members.empty() ? Assignment{} : assignor.assign(members, partitions, previous);
    std::vector<OwnershipChange> diff;
    for (const auto& [tp, ownership] : group.ownership) {
        if (!next.contains(tp)) {
            diff.push_back({tp, std::nullopt, 0});  // nobody owns it any more
        }
    }
    for (const auto& [tp, owner] : next) {
        const auto old = group.ownership.find(tp);
        if (assignor.eager() || old == group.ownership.end() || old->second.owner != owner) {
            diff.push_back({tp, owner, generation});
        }
    }
    return diff;
}

void GroupManager::replyAssignments(Group& group, TimeMs now) {
    for (auto& [id, member] : group.members) {
        JoinGroupResponse response{ErrorCode::NONE, id, group.generation, group.assignor, {}};
        for (const auto& [tp, ownership] : group.ownership) {
            if (ownership.owner == id) {
                response.assignment.push_back({tp.topic, tp.partition, ownership.epoch});
            }
        }
        if (member.pendingJoin) {
            member.pendingJoin->complete(encode(response, member.pendingJoinCorrelationId));
            member.pendingJoin.reset();
        }
        member.lastHeartbeatMs = now;
    }
}

// R-20: unique across restarts without coordination.
MemberId GroupManager::newMemberId(const std::string& clientId) {
    char suffix[17];
    std::snprintf(suffix, sizeof(suffix), "%016llx", static_cast<unsigned long long>(random_()));
    return clientId + "-" + suffix;
}

}  // namespace elog
