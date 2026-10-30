#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "elog/common/result.h"
#include "elog/common/types.h"
#include "elog/storage/segment.h"

namespace elog {

struct AppendInfo {
    Offset baseOffset = 0;  // offset of the batch's first record
    Offset lastOffset = 0;  // offset of its last record
};

struct ReadResult {
    std::vector<std::byte> batches;  // whole record batches, back to back
};

// The append-only log of one partition replica (LLD §5). This first version keeps a
// single segment; rolling, the sparse index, recovery and the leader-epoch cache come later.
//
// Thread rules: append() has one caller at a time (it runs under Replica::mu_). read(),
// leo() and logStartOffset() are lock-free and safe from any thread at any time.
class PartitionLog {
public:
    // Creates the log in a new directory dir (made if missing), starting at offset 0.
    static Result<std::unique_ptr<PartitionLog>> open(const std::string& dir);

    // Validates and appends one batch (LLD §5.3).
    //   assignOffsets = true  (leader):   stamps baseOffset = leo() and leaderEpoch = epoch.
    //   assignOffsets = false (follower): the batch must already start at leo().
    Result<AppendInfo> append(std::span<std::byte> batchBytes, Epoch epoch, bool assignOffsets);

    // Whole batches holding offsets from `from` onwards, skipping batches that start at or
    // after maxExclusive, up to about maxBytes (always at least one batch if any qualifies).
    // The first batch may start before `from`; callers slice it (LLD §5.4, §6.6).
    Result<ReadResult> read(Offset from, Offset maxExclusive, std::size_t maxBytes) const;

    // Log end offset: the offset the next appended record will get.
    Offset leo() const { return leo_.load(std::memory_order_acquire); }
    Offset logStartOffset() const { return segment_->baseOffset(); }

private:
    PartitionLog(std::string dir, std::shared_ptr<Segment> segment)
        : dir_(std::move(dir)), segment_(std::move(segment)) {}

    const std::string dir_;
    const std::shared_ptr<Segment> segment_;  // the only segment, for now
    // LLD §11.4: stored with release after the batch is written; readers load with acquire
    // and read only below it, so they never see a partly written batch.
    std::atomic<Offset> leo_{0};
};

}  // namespace elog
