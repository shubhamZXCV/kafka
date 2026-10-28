#include "elog/common/crc32c.h"

#include <crc32c/crc32c.h>

namespace elog {

std::uint32_t crc32c(std::span<const std::byte> data) {
    return crc32cExtend(0, data);
}

std::uint32_t crc32cExtend(std::uint32_t crc, std::span<const std::byte> data) {
    return crc32c::Extend(crc, reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
}

}  // namespace elog
