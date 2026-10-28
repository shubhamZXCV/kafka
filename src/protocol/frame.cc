#include "elog/protocol/frame.h"

#include <string>

#include "elog/common/bytes.h"
#include "elog/common/crc32c.h"

namespace elog {

namespace {

// Field offsets within the bytes after frameLength (LLD §4.1).
constexpr std::size_t kMagicAt = 0;
constexpr std::size_t kVersionAt = 1;
constexpr std::size_t kApiKeyAt = 2;
constexpr std::size_t kCorrelationIdAt = 4;
constexpr std::size_t kEncodingAt = 8;
constexpr std::size_t kFlagsAt = 9;

constexpr std::size_t kMinFrameLength = kFrameHeaderBytes + kFrameCrcBytes;  // empty body

}  // namespace

std::vector<std::byte> encodeFrame(const Frame& frame) {
    const auto frameLength =
        static_cast<std::uint32_t>(kFrameHeaderBytes + frame.body.size() + kFrameCrcBytes);
    std::vector<std::byte> out;
    out.reserve(sizeof(std::uint32_t) + frameLength);
    appendBE<std::uint32_t>(out, frameLength);
    appendBE<std::uint8_t>(out, kFrameMagic);
    appendBE<std::uint8_t>(out, kProtocolVersion);
    appendBE<std::uint16_t>(out, static_cast<std::uint16_t>(frame.apiKey));
    appendBE<std::uint32_t>(out, frame.correlationId);
    appendBE<std::uint8_t>(out, static_cast<std::uint8_t>(frame.encoding));
    appendBE<std::uint8_t>(out, frame.flags);
    appendBE<std::uint16_t>(out, 0);  // reserved
    out.insert(out.end(), frame.body.begin(), frame.body.end());
    // CRC over magic .. end of body, i.e. everything after frameLength so far.
    appendBE<std::uint32_t>(out, crc32c(std::span(out).subspan(sizeof(std::uint32_t))));
    return out;
}

Result<Frame> decodeFrame(std::span<const std::byte> afterLength) {
    if (afterLength.size() < kMinFrameLength) {
        return {ErrorCode::CORRUPT_MESSAGE, "frame too short"};
    }
    if (loadBE<std::uint8_t>(afterLength, kMagicAt) != kFrameMagic) {
        return {ErrorCode::CORRUPT_MESSAGE, "bad frame magic"};
    }
    const auto covered = afterLength.first(afterLength.size() - kFrameCrcBytes);
    const auto storedCrc = loadBE<std::uint32_t>(afterLength, covered.size());
    if (crc32c(covered) != storedCrc) {
        return {ErrorCode::CORRUPT_MESSAGE, "frame CRC mismatch"};
    }
    if (const auto version = loadBE<std::uint8_t>(afterLength, kVersionAt);
        version != kProtocolVersion) {
        return {ErrorCode::UNSUPPORTED_VERSION,
                "unsupported protocol version " + std::to_string(version)};
    }
    const auto encoding = loadBE<std::uint8_t>(afterLength, kEncodingAt);
    if (encoding != static_cast<std::uint8_t>(BodyEncoding::Json) &&
        encoding != static_cast<std::uint8_t>(BodyEncoding::MsgPack)) {
        return {ErrorCode::INVALID_REQUEST, "unknown body encoding " + std::to_string(encoding)};
    }

    Frame frame;
    frame.apiKey = static_cast<ApiKey>(loadBE<std::uint16_t>(afterLength, kApiKeyAt));
    frame.correlationId = loadBE<std::uint32_t>(afterLength, kCorrelationIdAt);
    frame.encoding = static_cast<BodyEncoding>(encoding);
    frame.flags = loadBE<std::uint8_t>(afterLength, kFlagsAt);
    const auto body = covered.subspan(kFrameHeaderBytes);
    frame.body.assign(body.begin(), body.end());
    return frame;
}

Result<Frame> readFrame(Socket& socket, std::size_t maxFrameBytes) {
    std::byte lengthBytes[sizeof(std::uint32_t)];
    if (auto status = socket.readExact(lengthBytes); !status.ok()) {
        return {status.error(), status.message()};
    }
    const std::uint32_t frameLength = loadBE<std::uint32_t>(lengthBytes, 0);
    // Validate before allocating: a garbage length must not make us reserve gigabytes.
    if (frameLength < kMinFrameLength || frameLength > maxFrameBytes) {
        return {ErrorCode::CORRUPT_MESSAGE, "invalid frame length " + std::to_string(frameLength)};
    }
    std::vector<std::byte> rest(frameLength);
    if (auto status = socket.readExact(rest); !status.ok()) {
        return {status.error(), status.message()};
    }
    return decodeFrame(rest);
}

Status writeFrame(Socket& socket, const Frame& frame) {
    return socket.writeAll(encodeFrame(frame));
}

}  // namespace elog
