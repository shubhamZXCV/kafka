#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "elog/common/result.h"

namespace elog {

struct IndexEntry {
    std::uint32_t relativeOffset = 0;  // batch base offset - segment base offset
    std::uint32_t position = 0;        // byte position of that batch in the .log file
};

// The sparse offset index of one segment (LLD §5.2.2): 8-byte entries (u32 BE relative
// offset, u32 BE position) in a .index file, mirrored in memory. An entry is written for a
// segment's first batch and then whenever index.interval.bytes have been appended since
// the previous entry, so a lookup lands at most that many bytes before its target.
//
// Thread rules: the partition's writer calls maybeAppend(), truncateAfterPos() and flush();
// readers call floor() at any time. mu_ is a leaf lock: nothing else is taken while it is
// held (R-19).
class SparseIndex {
public:
    // Opens the index at `path`, creating an empty one if it does not exist. A file whose
    // length is not a multiple of 8 or whose entries are not strictly increasing is
    // CORRUPT_MESSAGE; the caller then rebuilds it from the log.
    static Result<std::unique_ptr<SparseIndex>> open(const std::string& path,
                                                     std::uint32_t intervalBytes);

    SparseIndex(const SparseIndex&) = delete;
    SparseIndex& operator=(const SparseIndex&) = delete;
    ~SparseIndex();

    // Called before a batch of `batchBytes` is written at `position`.
    Status maybeAppend(std::uint32_t relativeOffset, std::uint32_t position,
                       std::uint32_t batchBytes);

    // The last entry with relativeOffset <= target, or {0, 0} (the segment start) if none.
    IndexEntry floor(std::uint32_t relativeOffset) const;

    // Drops entries at or after `position` (the log was truncated there).
    Status truncateAfterPos(std::uint32_t position);

    // Whether every entry points inside a log of `logSize` bytes (LLD §5.6).
    bool fitsLog(std::uint32_t logSize) const;

    Status flush();
    std::size_t entryCount() const;

private:
    SparseIndex(int fd, std::string path, std::uint32_t intervalBytes,
                std::vector<IndexEntry> entries)
        : fd_(fd),
          path_(std::move(path)),
          intervalBytes_(intervalBytes),
          entries_(std::move(entries)) {}

    const int fd_;
    const std::string path_;
    const std::uint32_t intervalBytes_;
    mutable std::mutex mu_;  // guards entries_ and bytesSinceLast_
    std::vector<IndexEntry> entries_;
    std::uint32_t bytesSinceLast_ = 0;
};

}  // namespace elog
