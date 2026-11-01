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
    // Creates a new, empty segment file and its index. Fails if the file already exists:
    // reopening existing segments needs crash recovery, which arrives with LLD §5.6.
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

    SparseIndex& index() const { return *index_; }
    TimeMs createdMs() const { return createdMs_; }  // monotonic, for segment.ms

private:
    Segment(int fd, Offset baseOffset, std::string path, std::unique_ptr<SparseIndex> index,
            TimeMs createdMs)
        : fd_(fd),
          baseOffset_(baseOffset),
          path_(std::move(path)),
          index_(std::move(index)),
          createdMs_(createdMs) {}

    const int fd_;
    const Offset baseOffset_;
    const std::string path_;  // for error messages
    const std::unique_ptr<SparseIndex> index_;
    const TimeMs createdMs_;
    std::atomic<std::uint32_t> size_{0};
};

// The file names of the segment starting at baseOffset (LLD §5.2.1, §5.2.2).
std::string segmentFileName(Offset baseOffset);
std::string indexFileName(Offset baseOffset);

}  // namespace elog
