#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "elog/common/result.h"
#include "elog/common/types.h"

namespace elog {

// The application value elog-producer writes and the analysis reads (LLD §4.5):
// a MessagePack map {"pid": str, "seq": uint, "ts": int (wall ms), "pad": bin}.
struct AppValue {
    std::string pid;
    std::uint64_t seq = 0;
    TimeMs ts = 0;
    std::size_t padBytes = 0;  // the pad's length; its content is not meaningful
};

std::vector<std::byte> encodeAppValue(const AppValue& value);
Result<AppValue> decodeAppValue(std::span<const std::byte> bytes);

}  // namespace elog
