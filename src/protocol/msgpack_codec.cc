#include "elog/protocol/msgpack_codec.h"

#include <concepts>
#include <exception>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

#include <msgpack.hpp>

namespace elog {

namespace {

// --- Encoding -----------------------------------------------------------------------------

// Lets msgpack::packer append straight into a byte vector.
struct ByteSink {
    std::vector<std::byte>& out;

    void write(const char* data, std::size_t size) {
        const auto* bytes = reinterpret_cast<const std::byte*>(data);
        out.insert(out.end(), bytes, bytes + size);
    }
};

using Packer = msgpack::packer<ByteSink>;

void packKey(Packer& packer, std::string_view name) {
    packer.pack_str(static_cast<std::uint32_t>(name.size()));
    packer.pack_str_body(name.data(), static_cast<std::uint32_t>(name.size()));
}

template <class T>
void packField(Packer& packer, std::string_view name, const T& value) {
    packKey(packer, name);
    packer.pack(value);
}

void packErrorField(Packer& packer, ErrorCode error) {
    packField(packer, "error", static_cast<std::int16_t>(error));
}

void packRecordsField(Packer& packer, const std::vector<std::byte>& records) {
    packKey(packer, "records");
    packer.pack_bin(static_cast<std::uint32_t>(records.size()));
    packer.pack_bin_body(reinterpret_cast<const char*>(records.data()),
                         static_cast<std::uint32_t>(records.size()));
}

void packArrayKey(Packer& packer, std::string_view name, std::size_t size) {
    packKey(packer, name);
    packer.pack_array(static_cast<std::uint32_t>(size));
}

Frame makeFrame(ApiKey apiKey, std::uint32_t correlationId, bool isResponse,
                std::vector<std::byte> body) {
    Frame frame;
    frame.apiKey = apiKey;
    frame.correlationId = correlationId;
    frame.encoding = BodyEncoding::MsgPack;
    frame.flags = isResponse ? kFlagResponse : 0;
    frame.body = std::move(body);
    return frame;
}

// --- Decoding -----------------------------------------------------------------------------
//
// The readers throw DecodeError; decodeBody() turns it into INVALID_REQUEST, so no exception
// leaves this file.

using Object = msgpack::object;

struct DecodeError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Our largest map has 6 fields; a little slack, but no huge maps from a hostile peer.
constexpr std::size_t kMaxMapEntries = 16;
// Body map -> partitions array -> partition map -> field value.
constexpr std::size_t kMaxDepth = 4;

const Object* findField(const Object& map, std::string_view name) {
    if (map.type != msgpack::type::MAP) {
        throw DecodeError("expected a map holding '" + std::string(name) + "'");
    }
    for (const auto& kv : std::span(map.via.map.ptr, map.via.map.size)) {
        if (kv.key.type == msgpack::type::STR &&
            std::string_view(kv.key.via.str.ptr, kv.key.via.str.size) == name) {
            return &kv.val;
        }
    }
    return nullptr;
}

const Object& field(const Object& map, const char* name) {
    const Object* value = findField(map, name);
    if (value == nullptr) {
        throw DecodeError(std::string("missing field '") + name + "'");
    }
    return *value;
}

// An integer that must fit T: rejects other types and out-of-range values (including
// negatives in unsigned fields).
template <std::integral T>
T toInt(const Object& value, const char* name) {
    using Limits = std::numeric_limits<T>;
    if (value.type == msgpack::type::POSITIVE_INTEGER) {
        if (value.via.u64 <= static_cast<std::uint64_t>(Limits::max())) {
            return static_cast<T>(value.via.u64);
        }
    } else if (value.type == msgpack::type::NEGATIVE_INTEGER) {
        if (std::is_signed_v<T> && value.via.i64 >= static_cast<std::int64_t>(Limits::min())) {
            return static_cast<T>(value.via.i64);
        }
    } else {
        throw DecodeError(std::string("'") + name + "' is not an integer");
    }
    throw DecodeError(std::string("'") + name + "' is out of range");
}

template <std::integral T>
T intField(const Object& map, const char* name) {
    return toInt<T>(field(map, name), name);
}

ErrorCode errorField(const Object& map) {
    return static_cast<ErrorCode>(intField<std::int16_t>(map, "error"));
}

std::string stringField(const Object& map, const char* name) {
    const Object& value = field(map, name);
    if (value.type != msgpack::type::STR) {
        throw DecodeError(std::string("'") + name + "' is not a string");
    }
    return std::string(value.via.str.ptr, value.via.str.size);
}

std::vector<std::byte> recordsField(const Object& map) {
    const Object& value = field(map, "records");
    if (value.type != msgpack::type::BIN) {
        throw DecodeError("'records' is not bin");
    }
    const auto* bytes = reinterpret_cast<const std::byte*>(value.via.bin.ptr);
    return {bytes, bytes + value.via.bin.size};
}

// The array length is already bounded by the unpack limit in decodeBody().
std::span<const Object> arrayField(const Object& map, const char* name) {
    const Object& value = field(map, name);
    if (value.type != msgpack::type::ARRAY) {
        throw DecodeError(std::string("'") + name + "' is not an array");
    }
    return {value.via.array.ptr, value.via.array.size};
}

// Makes msgpack point strings and bins at the frame body instead of copying them; the
// readers above copy each one exactly once, into the message struct.
bool referenceBytes(msgpack::type::object_type type, std::size_t /*size*/, void* /*userData*/) {
    return type == msgpack::type::STR || type == msgpack::type::BIN;
}

// Checks the frame, unpacks the body and runs read() on it.
template <class T, class Read>
Result<T> decodeBody(const Frame& frame, ApiKey apiKey, Read read) {
    const std::string what(apiKeyName(apiKey));
    if (frame.apiKey != apiKey || frame.encoding != BodyEncoding::MsgPack) {
        return {ErrorCode::INVALID_REQUEST, "expected a MessagePack " + what + " frame"};
    }
    const char* data = reinterpret_cast<const char*>(frame.body.data());
    const std::size_t size = frame.body.size();
    // Bounds what hostile length prefixes can make msgpack allocate (LLD §4.7).
    const msgpack::unpack_limit limit(/*array=*/kMaxRequestEntries, /*map=*/kMaxMapEntries,
                                      /*str=*/size, /*bin=*/size, /*ext=*/0, kMaxDepth);
    try {
        std::size_t used = 0;
        const auto handle = msgpack::unpack(data, size, used, referenceBytes, nullptr, limit);
        if (used != size) {
            throw DecodeError("unexpected bytes after the body");
        }
        return read(handle.get());
    } catch (const std::exception& e) {  // DecodeError, or malformed / over-limit MessagePack
        return {ErrorCode::INVALID_REQUEST, what + ": " + e.what()};
    }
}

}  // namespace

// --- Produce (10) -------------------------------------------------------------------------

Frame encode(const ProduceRequest& request, std::uint32_t correlationId) {
    std::vector<std::byte> body;
    ByteSink sink{body};
    Packer packer(sink);
    packer.pack_map(2);
    packField(packer, "timeoutMs", request.timeoutMs);
    packArrayKey(packer, "partitions", request.partitions.size());
    for (const auto& p : request.partitions) {
        packer.pack_map(4);
        packField(packer, "topic", p.topic);
        packField(packer, "partition", p.partition);
        packField(packer, "leaderEpoch", p.leaderEpoch);
        packRecordsField(packer, p.records);
    }
    return makeFrame(ApiKey::Produce, correlationId, false, std::move(body));
}

Frame encode(const ProduceResponse& response, std::uint32_t correlationId) {
    std::vector<std::byte> body;
    ByteSink sink{body};
    Packer packer(sink);
    packer.pack_map(2);
    packErrorField(packer, response.error);
    packArrayKey(packer, "partitions", response.partitions.size());
    for (const auto& p : response.partitions) {
        const bool throttled = p.throttleMs != 0;
        packer.pack_map(throttled ? 5 : 4);
        packField(packer, "topic", p.topic);
        packField(packer, "partition", p.partition);
        packErrorField(packer, p.error);
        packField(packer, "baseOffset", p.baseOffset);
        if (throttled) {
            packField(packer, "throttleMs", p.throttleMs);
        }
    }
    return makeFrame(ApiKey::Produce, correlationId, true, std::move(body));
}

Result<ProduceRequest> decodeProduceRequest(const Frame& frame) {
    return decodeBody<ProduceRequest>(frame, ApiKey::Produce, [](const Object& body) {
        ProduceRequest request;
        request.timeoutMs = intField<std::uint32_t>(body, "timeoutMs");
        for (const Object& p : arrayField(body, "partitions")) {
            request.partitions.push_back({stringField(p, "topic"),
                                          intField<std::uint32_t>(p, "partition"),
                                          intField<Epoch>(p, "leaderEpoch"), recordsField(p)});
        }
        return request;
    });
}

Result<ProduceResponse> decodeProduceResponse(const Frame& frame) {
    return decodeBody<ProduceResponse>(frame, ApiKey::Produce, [](const Object& body) {
        ProduceResponse response;
        response.error = errorField(body);
        for (const Object& p : arrayField(body, "partitions")) {
            const Object* throttle = findField(p, "throttleMs");  // optional
            response.partitions.push_back(
                {stringField(p, "topic"), intField<std::uint32_t>(p, "partition"), errorField(p),
                 intField<Offset>(p, "baseOffset"),
                 throttle != nullptr ? toInt<std::uint32_t>(*throttle, "throttleMs") : 0});
        }
        return response;
    });
}

// --- Fetch (11) ---------------------------------------------------------------------------

Frame encode(const FetchRequest& request, std::uint32_t correlationId) {
    std::vector<std::byte> body;
    ByteSink sink{body};
    Packer packer(sink);
    packer.pack_map(4);
    packField(packer, "maxWaitMs", request.maxWaitMs);
    packField(packer, "maxBytes", request.maxBytes);
    packField(packer, "credit", request.credit);
    packArrayKey(packer, "partitions", request.partitions.size());
    for (const auto& p : request.partitions) {
        packer.pack_map(4);
        packField(packer, "topic", p.topic);
        packField(packer, "partition", p.partition);
        packField(packer, "fetchOffset", p.fetchOffset);
        packField(packer, "currentLeaderEpoch", p.currentLeaderEpoch);
    }
    return makeFrame(ApiKey::Fetch, correlationId, false, std::move(body));
}

Frame encode(const FetchResponse& response, std::uint32_t correlationId) {
    std::vector<std::byte> body;
    ByteSink sink{body};
    Packer packer(sink);
    packer.pack_map(2);
    packErrorField(packer, response.error);
    packArrayKey(packer, "partitions", response.partitions.size());
    for (const auto& p : response.partitions) {
        packer.pack_map(6);
        packField(packer, "topic", p.topic);
        packField(packer, "partition", p.partition);
        packErrorField(packer, p.error);
        packField(packer, "highWatermark", p.highWatermark);
        packField(packer, "logStartOffset", p.logStartOffset);
        packRecordsField(packer, p.records);
    }
    return makeFrame(ApiKey::Fetch, correlationId, true, std::move(body));
}

Result<FetchRequest> decodeFetchRequest(const Frame& frame) {
    return decodeBody<FetchRequest>(frame, ApiKey::Fetch, [](const Object& body) {
        FetchRequest request;
        request.maxWaitMs = intField<std::uint32_t>(body, "maxWaitMs");
        request.maxBytes = intField<std::uint32_t>(body, "maxBytes");
        request.credit = intField<std::uint32_t>(body, "credit");
        for (const Object& p : arrayField(body, "partitions")) {
            request.partitions.push_back(
                {stringField(p, "topic"), intField<std::uint32_t>(p, "partition"),
                 intField<Offset>(p, "fetchOffset"), intField<Epoch>(p, "currentLeaderEpoch")});
        }
        return request;
    });
}

Result<FetchResponse> decodeFetchResponse(const Frame& frame) {
    return decodeBody<FetchResponse>(frame, ApiKey::Fetch, [](const Object& body) {
        FetchResponse response;
        response.error = errorField(body);
        for (const Object& p : arrayField(body, "partitions")) {
            response.partitions.push_back({stringField(p, "topic"),
                                           intField<std::uint32_t>(p, "partition"), errorField(p),
                                           intField<Offset>(p, "highWatermark"),
                                           intField<Offset>(p, "logStartOffset"), recordsField(p)});
        }
        return response;
    });
}

// --- Error-only response -----------------------------------------------------------------

Frame encodeMsgPackErrorResponse(ApiKey apiKey, std::uint32_t correlationId, ErrorCode error) {
    std::vector<std::byte> body;
    ByteSink sink{body};
    Packer packer(sink);
    packer.pack_map(1);
    packErrorField(packer, error);
    return makeFrame(apiKey, correlationId, true, std::move(body));
}

}  // namespace elog
