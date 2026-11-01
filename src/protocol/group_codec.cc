#include "elog/protocol/group_codec.h"

#include "json_fields.h"

namespace elog {

namespace {

using nlohmann::json;
using namespace json_fields;

std::vector<std::string> stringsField(const json& object, const char* name) {
    std::vector<std::string> out;
    for (const json& s : arrayField(object, name)) {
        if (!s.is_string()) {
            throw DecodeError(std::string("'") + name + "' holds a non-string");
        }
        out.push_back(s.get<std::string>());
    }
    return out;
}

json toJson(const PartitionEpoch& p) {
    return {{"topic", p.topic}, {"partition", p.partition}, {"epoch", p.epoch}};
}

std::vector<PartitionEpoch> partitionEpochsField(const json& object, const char* name) {
    std::vector<PartitionEpoch> out;
    for (const json& p : arrayField(object, name)) {
        out.push_back({stringField(p, "topic"), intField<std::uint32_t>(p, "partition"),
                       intField<Epoch>(p, "epoch")});
    }
    return out;
}

json toJsonArray(const std::vector<PartitionEpoch>& items) {
    json out = json::array();
    for (const auto& item : items) {
        out.push_back(toJson(item));
    }
    return out;
}

json errorOnly(ErrorCode error) {
    return {{"error", errorJson(error)}};
}

}  // namespace

// --- JoinGroup (20) -------------------------------------------------------------------------

Frame encode(const JoinGroupRequest& r, std::uint32_t correlationId) {
    const json body = {{"groupId", r.groupId},
                       {"memberId", r.memberId},
                       {"clientId", r.clientId},
                       {"topics", r.topics},
                       {"sessionTimeoutMs", r.sessionTimeoutMs},
                       {"rebalanceTimeoutMs", r.rebalanceTimeoutMs},
                       {"owned", toJsonArray(r.owned)}};
    return makeFrame(ApiKey::JoinGroup, correlationId, false, body);
}
Frame encode(const JoinGroupResponse& r, std::uint32_t correlationId) {
    const json body = {{"error", errorJson(r.error)},
                       {"memberId", r.memberId},
                       {"generation", r.generation},
                       {"assignor", r.assignor},
                       {"assignment", toJsonArray(r.assignment)}};
    return makeFrame(ApiKey::JoinGroup, correlationId, true, body);
}
Result<JoinGroupRequest> decodeJoinGroupRequest(const Frame& frame) {
    return decodeBody<JoinGroupRequest>(frame, ApiKey::JoinGroup, [](const json& j) {
        return JoinGroupRequest{stringField(j, "groupId"),
                                stringField(j, "memberId"),
                                stringField(j, "clientId"),
                                stringsField(j, "topics"),
                                intField<std::uint32_t>(j, "sessionTimeoutMs"),
                                intField<std::uint32_t>(j, "rebalanceTimeoutMs"),
                                partitionEpochsField(j, "owned")};
    });
}
Result<JoinGroupResponse> decodeJoinGroupResponse(const Frame& frame) {
    return decodeBody<JoinGroupResponse>(frame, ApiKey::JoinGroup, [](const json& j) {
        return JoinGroupResponse{errorField(j), stringField(j, "memberId"),
                                 intField<Generation>(j, "generation"), stringField(j, "assignor"),
                                 partitionEpochsField(j, "assignment")};
    });
}

// --- Heartbeat (21) / LeaveGroup (22) -------------------------------------------------------

Frame encode(const HeartbeatRequest& r, std::uint32_t correlationId) {
    const json body = {
        {"groupId", r.groupId}, {"memberId", r.memberId}, {"generation", r.generation}};
    return makeFrame(ApiKey::Heartbeat, correlationId, false, body);
}
Frame encode(const HeartbeatResponse& r, std::uint32_t correlationId) {
    return makeFrame(ApiKey::Heartbeat, correlationId, true, errorOnly(r.error));
}
Result<HeartbeatRequest> decodeHeartbeatRequest(const Frame& frame) {
    return decodeBody<HeartbeatRequest>(frame, ApiKey::Heartbeat, [](const json& j) {
        return HeartbeatRequest{stringField(j, "groupId"), stringField(j, "memberId"),
                                intField<Generation>(j, "generation")};
    });
}
Result<HeartbeatResponse> decodeHeartbeatResponse(const Frame& frame) {
    return decodeBody<HeartbeatResponse>(
        frame, ApiKey::Heartbeat, [](const json& j) { return HeartbeatResponse{errorField(j)}; });
}

Frame encode(const LeaveGroupRequest& r, std::uint32_t correlationId) {
    const json body = {{"groupId", r.groupId}, {"memberId", r.memberId}};
    return makeFrame(ApiKey::LeaveGroup, correlationId, false, body);
}
Frame encode(const LeaveGroupResponse& r, std::uint32_t correlationId) {
    return makeFrame(ApiKey::LeaveGroup, correlationId, true, errorOnly(r.error));
}
Result<LeaveGroupRequest> decodeLeaveGroupRequest(const Frame& frame) {
    return decodeBody<LeaveGroupRequest>(frame, ApiKey::LeaveGroup, [](const json& j) {
        return LeaveGroupRequest{stringField(j, "groupId"), stringField(j, "memberId")};
    });
}
Result<LeaveGroupResponse> decodeLeaveGroupResponse(const Frame& frame) {
    return decodeBody<LeaveGroupResponse>(
        frame, ApiKey::LeaveGroup, [](const json& j) { return LeaveGroupResponse{errorField(j)}; });
}

// --- OffsetCommit (23) ----------------------------------------------------------------------

Frame encode(const OffsetCommitRequest& r, std::uint32_t correlationId) {
    json offsets = json::array();
    for (const auto& e : r.offsets) {
        offsets.push_back({{"topic", e.topic},
                           {"partition", e.partition},
                           {"offset", e.offset},
                           {"epoch", e.epoch}});
    }
    const json body = {{"groupId", r.groupId}, {"memberId", r.memberId}, {"offsets", offsets}};
    return makeFrame(ApiKey::OffsetCommit, correlationId, false, body);
}
Frame encode(const OffsetCommitResponse& r, std::uint32_t correlationId) {
    json offsets = json::array();
    for (const auto& e : r.offsets) {
        offsets.push_back(
            {{"topic", e.topic}, {"partition", e.partition}, {"error", errorJson(e.error)}});
    }
    const json body = {{"error", errorJson(r.error)}, {"offsets", offsets}};
    return makeFrame(ApiKey::OffsetCommit, correlationId, true, body);
}
Result<OffsetCommitRequest> decodeOffsetCommitRequest(const Frame& frame) {
    return decodeBody<OffsetCommitRequest>(frame, ApiKey::OffsetCommit, [](const json& j) {
        OffsetCommitRequest r{stringField(j, "groupId"), stringField(j, "memberId"), {}};
        for (const json& e : arrayField(j, "offsets")) {
            r.offsets.push_back({stringField(e, "topic"), intField<std::uint32_t>(e, "partition"),
                                 intField<Offset>(e, "offset"), intField<Epoch>(e, "epoch")});
        }
        return r;
    });
}
Result<OffsetCommitResponse> decodeOffsetCommitResponse(const Frame& frame) {
    return decodeBody<OffsetCommitResponse>(frame, ApiKey::OffsetCommit, [](const json& j) {
        OffsetCommitResponse r{errorField(j), {}};
        for (const json& e : arrayField(j, "offsets")) {
            r.offsets.push_back(
                {stringField(e, "topic"), intField<std::uint32_t>(e, "partition"), errorField(e)});
        }
        return r;
    });
}

// --- OffsetFetch (24) -----------------------------------------------------------------------

Frame encode(const OffsetFetchRequest& r, std::uint32_t correlationId) {
    json partitions = json::array();
    for (const auto& tp : r.partitions) {
        partitions.push_back({{"topic", tp.topic}, {"partition", tp.partition}});
    }
    const json body = {{"groupId", r.groupId}, {"partitions", partitions}};
    return makeFrame(ApiKey::OffsetFetch, correlationId, false, body);
}
Frame encode(const OffsetFetchResponse& r, std::uint32_t correlationId) {
    json offsets = json::array();
    for (const auto& e : r.offsets) {
        offsets.push_back({{"topic", e.topic},
                           {"partition", e.partition},
                           {"offset", e.offset},
                           {"error", errorJson(e.error)}});
    }
    const json body = {{"error", errorJson(r.error)}, {"offsets", offsets}};
    return makeFrame(ApiKey::OffsetFetch, correlationId, true, body);
}
Result<OffsetFetchRequest> decodeOffsetFetchRequest(const Frame& frame) {
    return decodeBody<OffsetFetchRequest>(frame, ApiKey::OffsetFetch, [](const json& j) {
        OffsetFetchRequest r{stringField(j, "groupId"), {}};
        for (const json& p : arrayField(j, "partitions")) {
            r.partitions.push_back(
                {stringField(p, "topic"), intField<std::uint32_t>(p, "partition")});
        }
        return r;
    });
}
Result<OffsetFetchResponse> decodeOffsetFetchResponse(const Frame& frame) {
    return decodeBody<OffsetFetchResponse>(frame, ApiKey::OffsetFetch, [](const json& j) {
        OffsetFetchResponse r{errorField(j), {}};
        for (const json& e : arrayField(j, "offsets")) {
            r.offsets.push_back({stringField(e, "topic"), intField<std::uint32_t>(e, "partition"),
                                 intField<std::int64_t>(e, "offset"), errorField(e)});
        }
        return r;
    });
}

// --- DescribeGroup (25) ---------------------------------------------------------------------

Frame encode(const DescribeGroupRequest& r, std::uint32_t correlationId) {
    return makeFrame(ApiKey::DescribeGroup, correlationId, false, {{"groupId", r.groupId}});
}
Frame encode(const DescribeGroupResponse& r, std::uint32_t correlationId) {
    json members = json::array(), partitions = json::array();
    for (const auto& m : r.members) {
        members.push_back({{"memberId", m.memberId}, {"clientId", m.clientId}});
    }
    for (const auto& p : r.partitions) {
        partitions.push_back({{"topic", p.topic},
                              {"partition", p.partition},
                              {"owner", p.owner},
                              {"epoch", p.epoch},
                              {"committedOffset", p.committedOffset}});
    }
    const json body = {{"error", errorJson(r.error)}, {"state", r.state},
                       {"generation", r.generation},  {"assignor", r.assignor},
                       {"members", members},          {"partitions", partitions}};
    return makeFrame(ApiKey::DescribeGroup, correlationId, true, body);
}
Result<DescribeGroupRequest> decodeDescribeGroupRequest(const Frame& frame) {
    return decodeBody<DescribeGroupRequest>(frame, ApiKey::DescribeGroup, [](const json& j) {
        return DescribeGroupRequest{stringField(j, "groupId")};
    });
}
Result<DescribeGroupResponse> decodeDescribeGroupResponse(const Frame& frame) {
    return decodeBody<DescribeGroupResponse>(frame, ApiKey::DescribeGroup, [](const json& j) {
        DescribeGroupResponse r{errorField(j),
                                stringField(j, "state"),
                                intField<Generation>(j, "generation"),
                                stringField(j, "assignor"),
                                {},
                                {}};
        for (const json& m : arrayField(j, "members")) {
            r.members.push_back({stringField(m, "memberId"), stringField(m, "clientId")});
        }
        for (const json& p : arrayField(j, "partitions")) {
            r.partitions.push_back({stringField(p, "topic"),
                                    intField<std::uint32_t>(p, "partition"),
                                    stringField(p, "owner"), intField<Epoch>(p, "epoch"),
                                    intField<std::int64_t>(p, "committedOffset")});
        }
        return r;
    });
}

}  // namespace elog
