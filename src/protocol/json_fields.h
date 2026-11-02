#pragma once

// Internal to the protocol module: the checked JSON helpers shared by the control-plane
// codecs (json_codec.cc, group_codec.cc). Not a public header.

#include <concepts>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "elog/common/result.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/messages.h"  // kMaxRequestEntries

namespace elog::json_fields {

using nlohmann::json;

inline Frame makeFrame(ApiKey apiKey, std::uint32_t correlationId, bool isResponse,
                       const json& body) {
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

inline json errorJson(ErrorCode error) {
    return static_cast<std::int16_t>(error);
}

struct DecodeError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

inline const json& field(const json& object, const char* name) {
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

inline ErrorCode errorField(const json& object) {
    return static_cast<ErrorCode>(intField<std::int16_t>(object, "error"));
}

inline std::string stringField(const json& object, const char* name) {
    const json& value = field(object, name);
    if (!value.is_string()) {
        throw DecodeError(std::string("'") + name + "' is not a string");
    }
    return value.get<std::string>();
}

inline const json& arrayField(const json& object, const char* name) {
    const json& value = field(object, name);
    if (!value.is_array()) {
        throw DecodeError(std::string("'") + name + "' is not an array");
    }
    if (value.size() > kMaxRequestEntries) {
        throw DecodeError(std::string("'") + name + "' has too many entries");
    }
    return value;
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

}  // namespace elog::json_fields
