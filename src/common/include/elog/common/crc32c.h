#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace elog {

// CRC-32C (Castagnoli), hardware-accelerated where the CPU supports it. Protects frames
// (LLD §4.1) and record batches (LLD §4.5).
std::uint32_t crc32c(std::span<const std::byte> data);

// Continues a CRC over more data: crc32cExtend(crc32c(a), b) == crc32c(a + b).
std::uint32_t crc32cExtend(std::uint32_t crc, std::span<const std::byte> data);

}  // namespace elog
