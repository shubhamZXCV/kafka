#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "elog/common/types.h"

namespace elog {

// What log-verify found in one partition directory (LLD §15.2).
struct PartitionReport {
    std::string name;  // e.g. "orders-3"
    std::size_t segments = 0;
    std::uint64_t batches = 0;
    Offset firstOffset = 0;
    Offset nextOffset = 0;  // one past the last valid record
    std::vector<std::string> errors;
};

// Checks every partition directory under `dataDir` without changing anything: each batch
// parses (length, magic, CRC), offsets are contiguous within and across segments, and each
// segment's name matches its first batch. Only reads files, never opens them for writing.
std::vector<PartitionReport> verifyDataDir(const std::string& dataDir);

}  // namespace elog
