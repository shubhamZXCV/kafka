#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "elog/common/murmur3.h"

namespace elog {

// LLD §10.2, OI-1: the partition for a keyed record. Records without a key are spread
// round-robin by the producer (FR-PART-05).
inline std::uint32_t partitionFor(std::span<const std::byte> key, std::uint32_t numPartitions) {
    return murmur3_x86_32(key.data(), key.size(), 0x9747b28c) % numPartitions;
}

}  // namespace elog
