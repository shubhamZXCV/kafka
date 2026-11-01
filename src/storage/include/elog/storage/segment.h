#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "elog/common/result.h"
#include "elog/common/types.h"
#include "elog/storage/sparse_index.h"

namespace elog {

// One segment file of a partition log: record batches concatenated, with no file header
// (LLD §5.2.1). Named after its base offset, e.g. 00000000000000005000.log.
//
// Thread rules: one writer (the partition's appender, under Replica::mu_) calls append()
// and flush(); any number of readers may call size() and readAt() concurrently, reading
// only below a size() they have loaded.
class Segment {
public:
    // Creates a new, empty segment file and its index. Fails if the file already exists, so
    // data is never overwritten; existing segments are reopened with open().
    static Result<std::shared_ptr<Segment>> create(const std::string& dir, Offset baseOffset,
                                                   std::uint32_t indexIntervalBytes);

    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;
    ~Segment();

    Offset baseOffset() const { return baseOffset_; }
    // Bytes written so far. Published with release after the bytes are written.
    std::uint32_t size() const { return size_.load(std::memory_order_acquire); }

    // Writes bytes at the tail of the file, all or nothing.
    Status append(std::span<const std::byte> bytes);

    // Reads len bytes at pos into out (which is resized). pos + len must be <= size().
    Status readAt(std::uint64_t pos, std::size_t len, std::vector<std::byte>& out) const;

    // fdatasync: written bytes survive a power failure once this returns.
    Status flush();

    // Opens an existing segment file and its index (startup recovery, LLD §5.6).
    static Result<std::shared_ptr<Segment>> open(const std::string& dir, Offset baseOffset,
                                                 std::uint32_t indexIntervalBytes);

    // Cuts the file at `pos` and syncs it (recovery and truncation, LLD §5.5–5.6).
    Status truncateToPos(std::uint32_t pos);

    // Recreates the index from the batch headers (LLD §5.6). open() calls it for an index
    // that is invalid, points past the log, or is missing for a non-empty log.
    Status rebuildIndex();

    SparseIndex& index() const { return *index_; }
    const std::string& path() const { return path_; }
    std::string indexPath() const;
    TimeMs createdMs() const { return createdMs_; }  // monotonic, for segment.ms

    // Wall-clock time of the newest record, for retention.ms (LLD §5.7). For a segment
    // reopened at startup it starts as the file's modification time.
    TimeMs maxTimestampMs() const { return maxTimestampMs_.load(std::memory_order_relaxed); }
    void observeTimestamp(TimeMs ts);

private:
    Segment(int fd, Offset baseOffset, std::string path, std::unique_ptr<SparseIndex> index,
            TimeMs createdMs, std::uint32_t size)
        : fd_(fd),
          baseOffset_(baseOffset),
          path_(std::move(path)),
          index_(std::move(index)),
          createdMs_(createdMs),
          size_(size) {}
    static Result<std::shared_ptr<Segment>> fromFd(int fd, const std::string& dir,
                                                   Offset baseOffset, std::string path,
                                                   std::uint32_t indexIntervalBytes);

    const int fd_;
    const Offset baseOffset_;
    const std::string path_;  // for error messages
    const std::unique_ptr<SparseIndex> index_;
    const TimeMs createdMs_;
    std::atomic<std::uint32_t> size_;
    std::atomic<TimeMs> maxTimestampMs_{0};
};

// The file names of the segment starting at baseOffset (LLD §5.2.1, §5.2.2).
std::string segmentFileName(Offset baseOffset);
std::string indexFileName(Offset baseOffset);

}  // namespace elog
