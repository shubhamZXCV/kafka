#include "elog/protocol/replication_codec.h"

#include "json_fields.h"

namespace elog {

namespace {

using nlohmann::json;
using namespace json_fields;

std::vector<BrokerId> brokerIds(const json& object, const char* name) {
    std::vector<BrokerId> ids;
    for (const json& id : arrayField(object, name)) {
        ids.push_back(toInt<BrokerId>(id, name));
    }
    return ids;
}

}  // namespace

// --- AlterIsr (5) ---------------------------------------------------------------------------

Frame encode(const AlterIsrRequest& r, std::uint32_t correlationId) {
    const json body = {
        {"brokerId", r.brokerId},   {"incarnation", r.incarnation}, {"topic", r.topic},
        {"partition", r.partition}, {"leaderEpoch", r.leaderEpoch}, {"newIsr", r.newIsr}};
    return makeFrame(ApiKey::AlterIsr, correlationId, false, body);
}
Frame encode(const AlterIsrResponse& r, std::uint32_t correlationId) {
    const json body = {{"error", errorJson(r.error)}, {"isr", r.isr}};
    return makeFrame(ApiKey::AlterIsr, correlationId, true, body);
}
Result<AlterIsrRequest> decodeAlterIsrRequest(const Frame& frame) {
    return decodeBody<AlterIsrRequest>(frame, ApiKey::AlterIsr, [](const json& j) {
        return AlterIsrRequest{
            intField<BrokerId>(j, "brokerId"), intField<std::uint32_t>(j, "incarnation"),
            stringField(j, "topic"),           intField<std::uint32_t>(j, "partition"),
            intField<Epoch>(j, "leaderEpoch"), brokerIds(j, "newIsr")};
    });
}
Result<AlterIsrResponse> decodeAlterIsrResponse(const Frame& frame) {
    return decodeBody<AlterIsrResponse>(frame, ApiKey::AlterIsr, [](const json& j) {
        return AlterIsrResponse{errorField(j), brokerIds(j, "isr")};
    });
}

// --- OffsetsForLeaderEpoch (13) -------------------------------------------------------------

Frame encode(const OffsetsForLeaderEpochRequest& r, std::uint32_t correlationId) {
    json partitions = json::array();
    for (const auto& p : r.partitions) {
        partitions.push_back(
            {{"topic", p.topic}, {"partition", p.partition}, {"leaderEpoch", p.leaderEpoch}});
    }
    return makeFrame(ApiKey::OffsetsForLeaderEpoch, correlationId, false,
                     {{"partitions", partitions}});
}
Frame encode(const OffsetsForLeaderEpochResponse& r, std::uint32_t correlationId) {
    json partitions = json::array();
    for (const auto& p : r.partitions) {
        partitions.push_back({{"topic", p.topic},
                              {"partition", p.partition},
                              {"error", errorJson(p.error)},
                              {"leaderEpoch", p.leaderEpoch},
                              {"endOffset", p.endOffset}});
    }
    return makeFrame(ApiKey::OffsetsForLeaderEpoch, correlationId, true,
                     {{"error", errorJson(r.error)}, {"partitions", partitions}});
}
Result<OffsetsForLeaderEpochRequest> decodeOffsetsForLeaderEpochRequest(const Frame& frame) {
    return decodeBody<OffsetsForLeaderEpochRequest>(
        frame, ApiKey::OffsetsForLeaderEpoch, [](const json& j) {
            OffsetsForLeaderEpochRequest r;
            for (const json& p : arrayField(j, "partitions")) {
                r.partitions.push_back({stringField(p, "topic"),
                                        intField<std::uint32_t>(p, "partition"),
                                        intField<Epoch>(p, "leaderEpoch")});
            }
            return r;
        });
}
Result<OffsetsForLeaderEpochResponse> decodeOffsetsForLeaderEpochResponse(const Frame& frame) {
    return decodeBody<OffsetsForLeaderEpochResponse>(
        frame, ApiKey::OffsetsForLeaderEpoch, [](const json& j) {
            OffsetsForLeaderEpochResponse r{errorField(j), {}};
            for (const json& p : arrayField(j, "partitions")) {
                r.partitions.push_back({stringField(p, "topic"),
                                        intField<std::uint32_t>(p, "partition"), errorField(p),
                                        intField<Epoch>(p, "leaderEpoch"),
                                        intField<Offset>(p, "endOffset")});
            }
            return r;
        });
}

}  // namespace elog
