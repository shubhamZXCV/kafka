#include "elog/protocol/json_codec.h"

#include <concepts>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

namespace elog {

namespace {

using nlohmann::json;

// --- Encoding -----------------------------------------------------------------------------

Frame makeFrame(ApiKey apiKey, std::uint32_t correlationId, bool isResponse, const json& body) {
    // replace: a string with invalid UTF-8 is sent with U+FFFD instead of throwing.
    const std::string text = body.dump(-1, ' ', false, json::error_handler_t::replace);
    const auto bytes = std::as_bytes(std::span(text));
    Frame frame;
    frame.apiKey = apiKey;
    frame.correlationId = correlationId;
    frame.encoding = BodyEncoding::Json;
    frame.flags = isResponse ? kFlagResponse : 0;
    frame.body.assign(bytes.begin(), bytes.end());
    return frame;
}

json errorJson(ErrorCode error) {
    return static_cast<std::int16_t>(error);
}

json toJson(const BrokerAddress& broker) {
    return {{"id", broker.id}, {"host", broker.host}, {"port", broker.port}};
}

json toJson(const PartitionMetadata& p) {
    return {{"partition", p.partition},
            {"leader", p.leader},
            {"leaderEpoch", p.leaderEpoch},
            {"replicas", p.replicas},
            {"isr", p.isr}};
}

json toJson(const TopicMetadata& t) {
    json partitions = json::array();
    for (const auto& p : t.partitions) {
        partitions.push_back(toJson(p));
    }
    return {{"name", t.name}, {"error", errorJson(t.error)}, {"partitions", partitions}};
}

template <class T>
json toJsonArray(const std::vector<T>& items) {
    json out = json::array();
    for (const auto& item : items) {
        out.push_back(toJson(item));
    }
    return out;
}

// --- Decoding -----------------------------------------------------------------------------
//
// The readers throw DecodeError; decodeBody() turns it into INVALID_REQUEST, so no exception
// leaves this file.

struct DecodeError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

const json& field(const json& object, const char* name) {
    if (!object.is_object()) {
        throw DecodeError(std::string("expected an object holding '") + name + "'");
    }
    const auto it = object.find(name);
    if (it == object.end()) {
        throw DecodeError(std::string("missing field '") + name + "'");
    }
    return *it;
}

// An integer that must fit T: rejects floats, strings, and out-of-range values (including
// negatives in unsigned fields).
template <std::integral T>
T toInt(const json& value, const char* name) {
    using Limits = std::numeric_limits<T>;
    if (value.is_number_unsigned()) {
        const auto v = value.get<std::uint64_t>();
        if (v <= static_cast<std::uint64_t>(Limits::max())) {
            return static_cast<T>(v);
        }
    } else if (value.is_number_integer()) {
        const auto v = value.get<std::int64_t>();
        if (std::is_signed_v<T> && v >= static_cast<std::int64_t>(Limits::min()) &&
            v <= static_cast<std::int64_t>(Limits::max())) {
            return static_cast<T>(v);
        }
    } else {
        throw DecodeError(std::string("'") + name + "' is not an integer");
    }
    throw DecodeError(std::string("'") + name + "' is out of range");
}

template <std::integral T>
T intField(const json& object, const char* name) {
    return toInt<T>(field(object, name), name);
}

ErrorCode errorField(const json& object) {
    return static_cast<ErrorCode>(intField<std::int16_t>(object, "error"));
}

std::string stringField(const json& object, const char* name) {
    const json& value = field(object, name);
    if (!value.is_string()) {
        throw DecodeError(std::string("'") + name + "' is not a string");
    }
    return value.get<std::string>();
}

const json& arrayField(const json& object, const char* name) {
    const json& value = field(object, name);
    if (!value.is_array()) {
        throw DecodeError(std::string("'") + name + "' is not an array");
    }
    if (value.size() > kMaxRequestEntries) {
        throw DecodeError(std::string("'") + name + "' has too many entries");
    }
    return value;
}

std::vector<BrokerId> brokerIdsField(const json& object, const char* name) {
    std::vector<BrokerId> ids;
    for (const json& id : arrayField(object, name)) {
        ids.push_back(toInt<BrokerId>(id, name));
    }
    return ids;
}

BrokerAddress readBrokerAddress(const json& j) {
    return {intField<BrokerId>(j, "id"), stringField(j, "host"),
            intField<std::uint16_t>(j, "port")};
}

std::vector<BrokerAddress> brokersField(const json& object) {
    std::vector<BrokerAddress> brokers;
    for (const json& broker : arrayField(object, "brokers")) {
        brokers.push_back(readBrokerAddress(broker));
    }
    return brokers;
}

// Checks the frame, parses the body and runs read() on it.
template <class T, class Read>
Result<T> decodeBody(const Frame& frame, ApiKey apiKey, Read read) {
    const std::string what(apiKeyName(apiKey));
    if (frame.apiKey != apiKey || frame.encoding != BodyEncoding::Json) {
        return {ErrorCode::INVALID_REQUEST, "expected a JSON " + what + " frame"};
    }
    try {
        const auto* text = reinterpret_cast<const char*>(frame.body.data());
        return read(json::parse(text, text + frame.body.size()));
    } catch (const json::exception& e) {
        return {ErrorCode::INVALID_REQUEST, what + ": " + e.what()};
    } catch (const DecodeError& e) {
        return {ErrorCode::INVALID_REQUEST, what + ": " + e.what()};
    }
}

}  // namespace

// --- Metadata (1) -------------------------------------------------------------------------

Frame encode(const MetadataRequest& request, std::uint32_t correlationId) {
    return makeFrame(ApiKey::Metadata, correlationId, false, {{"topics", request.topics}});
}

Frame encode(const MetadataResponse& response, std::uint32_t correlationId) {
    const json body = {{"error", errorJson(response.error)},
                       {"brokers", toJsonArray(response.brokers)},
                       {"topics", toJsonArray(response.topics)}};
    return makeFrame(ApiKey::Metadata, correlationId, true, body);
}

Result<MetadataRequest> decodeMetadataRequest(const Frame& frame) {
    return decodeBody<MetadataRequest>(frame, ApiKey::Metadata, [](const json& j) {
        MetadataRequest request;
        for (const json& topic : arrayField(j, "topics")) {
            if (!topic.is_string()) {
                throw DecodeError("'topics' holds a non-string");
            }
            request.topics.push_back(topic.get<std::string>());
        }
        return request;
    });
}

Result<MetadataResponse> decodeMetadataResponse(const Frame& frame) {
    return decodeBody<MetadataResponse>(frame, ApiKey::Metadata, [](const json& j) {
        MetadataResponse response;
        response.error = errorField(j);
        response.brokers = brokersField(j);
        for (const json& t : arrayField(j, "topics")) {
            TopicMetadata topic{stringField(t, "name"), errorField(t), {}};
            for (const json& p : arrayField(t, "partitions")) {
                topic.partitions.push_back(
                    {intField<std::uint32_t>(p, "partition"), intField<BrokerId>(p, "leader"),
                     intField<Epoch>(p, "leaderEpoch"), brokerIdsField(p, "replicas"),
                     brokerIdsField(p, "isr")});
            }
            response.topics.push_back(std::move(topic));
        }
        return response;
    });
}

// --- CreateTopic (2) ----------------------------------------------------------------------

Frame encode(const CreateTopicRequest& request, std::uint32_t correlationId) {
    const json body = {{"name", request.name},
                       {"partitions", request.partitions},
                       {"replicationFactor", request.replicationFactor},
                       {"minIsr", request.minIsr}};
    return makeFrame(ApiKey::CreateTopic, correlationId, false, body);
}

Frame encode(const CreateTopicResponse& response, std::uint32_t correlationId) {
    return makeFrame(ApiKey::CreateTopic, correlationId, true,
                     {{"error", errorJson(response.error)}});
}

Result<CreateTopicRequest> decodeCreateTopicRequest(const Frame& frame) {
    return decodeBody<CreateTopicRequest>(frame, ApiKey::CreateTopic, [](const json& j) {
        return CreateTopicRequest{stringField(j, "name"), intField<std::uint32_t>(j, "partitions"),
                                  intField<std::uint32_t>(j, "replicationFactor"),
                                  intField<std::uint32_t>(j, "minIsr")};
    });
}

Result<CreateTopicResponse> decodeCreateTopicResponse(const Frame& frame) {
    return decodeBody<CreateTopicResponse>(frame, ApiKey::CreateTopic, [](const json& j) {
        return CreateTopicResponse{errorField(j)};
    });
}

}  // namespace elog
