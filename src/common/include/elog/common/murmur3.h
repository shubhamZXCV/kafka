#pragma once

#include <cstddef>
#include <cstdint>

// MurmurHash3 by Austin Appleby (public domain), vendored unchanged as src/common/murmur3.cc
// from https://github.com/aappleby/smhasher (src/MurmurHash3.cpp).
void MurmurHash3_x86_32(const void* key, int len, uint32_t seed, void* out);

namespace elog {

// The 32-bit x86 variant, used by the client partitioner (LLD §10.2, OI-1).
inline std::uint32_t murmur3_x86_32(const void* key, std::size_t len, std::uint32_t seed) {
    std::uint32_t hash = 0;
    MurmurHash3_x86_32(key, static_cast<int>(len), seed, &hash);
    return hash;
}

}  // namespace elog
