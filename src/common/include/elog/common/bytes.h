#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <span>
#include <vector>

// Big-endian integer encoding, used by every wire and disk format (LLD §1.2).
//
//     std::vector<std::byte> buf;
//     appendBE<std::uint32_t>(buf, frameLength);   // grow a buffer while encoding
//     storeBE<std::uint64_t>(batch, 0, baseOffset); // overwrite a field in place
//     auto len = loadBE<std::uint32_t>(header, 0);  // read a field
namespace elog {

template <std::unsigned_integral T>
void storeBE(std::span<std::byte> buf, std::size_t offset, T value) {
    assert(offset + sizeof(T) <= buf.size());
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        buf[offset + i] = static_cast<std::byte>(value >> (8 * (sizeof(T) - 1 - i)));
    }
}

template <std::unsigned_integral T>
T loadBE(std::span<const std::byte> buf, std::size_t offset) {
    assert(offset + sizeof(T) <= buf.size());
    T value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        value = static_cast<T>((value << 8) | std::to_integer<T>(buf[offset + i]));
    }
    return value;
}

template <std::unsigned_integral T>
void appendBE(std::vector<std::byte>& buf, T value) {
    buf.resize(buf.size() + sizeof(T));
    storeBE<T>(buf, buf.size() - sizeof(T), value);
}

}  // namespace elog
