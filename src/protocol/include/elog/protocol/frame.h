#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "elog/common/result.h"
#include "elog/common/socket.h"
#include "elog/protocol/api_keys.h"

namespace elog {

// The envelope around every request and response (LLD §4.1):
//
//     frameLength u32 | magic u8 | version u8 | apiKey u16 | correlationId u32
//     | encoding u8 | flags u8 | reserved u16 | body ... | crc32c u32
//
// frameLength counts the bytes after itself: 12 (fixed header) + body + 4 (CRC). The CRC
// covers magic through the end of the body. All integers are big-endian.

enum class BodyEncoding : std::uint8_t { Json = 0, MsgPack = 1 };

inline constexpr std::uint8_t kFrameMagic = 0xE1;
inline constexpr std::uint8_t kProtocolVersion = 1;
inline constexpr std::uint8_t kFlagResponse = 0x01;   // flags bit 0
inline constexpr std::size_t kFrameHeaderBytes = 12;  // magic .. reserved
inline constexpr std::size_t kFrameCrcBytes = 4;

struct Frame {
    ApiKey apiKey = ApiKey::Metadata;
    std::uint32_t correlationId = 0;  // chosen by the client, echoed in the response
    BodyEncoding encoding = BodyEncoding::Json;
    std::uint8_t flags = 0;
    std::vector<std::byte> body;

    bool isResponse() const { return (flags & kFlagResponse) != 0; }
};

// The complete frame bytes, frameLength included.
std::vector<std::byte> encodeFrame(const Frame& frame);

// Parses the bytes after frameLength. Errors: CORRUPT_MESSAGE (too short, bad magic, CRC
// mismatch), UNSUPPORTED_VERSION, INVALID_REQUEST (unknown body encoding).
Result<Frame> decodeFrame(std::span<const std::byte> afterLength);

// Reads one frame. frameLength is checked against maxFrameBytes *before* the body buffer is
// allocated, so a bogus length cannot exhaust memory (CORRUPT_MESSAGE). Socket failures are
// NETWORK_ERROR. On any error the caller should close the connection.
Result<Frame> readFrame(Socket& socket, std::size_t maxFrameBytes);

Status writeFrame(Socket& socket, const Frame& frame);

}  // namespace elog
