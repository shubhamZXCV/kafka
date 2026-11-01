#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
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

// log.flush.policy (LLD §5.8).
enum class FlushPolicy {
    BATCH,     // fdatasync after every append
    INTERVAL,  // the broker scheduler calls flush() every log.flush.interval.ms
    NONE,      // the OS decides; testing only
};

// The per-partition storage settings (LLD §12.1).
struct LogConfig {
    FlushPolicy flushPolicy = FlushPolicy::INTERVAL;
    std::uint32_t segmentBytes = 64 << 20;
    std::chrono::milliseconds segmentMs{604'800'000};
    std::uint32_t indexIntervalBytes = 4096;
};

// The append-only log of one partition replica (LLD §5): segments in offset order, the
// last one active. Recovery, retention and the leader-epoch cache come later.
//
// Thread rules: append() has one caller at a time (it runs under Replica::mu_). read(),
// leo() and logStartOffset() are safe from any thread at any time. segMu_ guards only the
// segments_ vector (lock order: Replica::mu_ → segMu_, LLD §11.2); reads copy it and do
// their I/O without it.
class PartitionLog {
public:
    // Creates the log in a new directory dir (made if missing), starting at offset 0.
    static Result<std::unique_ptr<PartitionLog>> open(const std::string& dir, LogConfig cfg);

    // Validates and appends one batch (LLD §5.3), rolling to a new segment first if the
    // active one is full or old enough.
    //   assignOffsets = true  (leader):   stamps baseOffset = leo() and leaderEpoch = epoch.
    //   assignOffsets = false (follower): the batch must already start at leo().
    Result<AppendInfo> append(std::span<std::byte> batchBytes, Epoch epoch, bool assignOffsets);

    // Whole batches holding offsets from `from` onwards, skipping batches that start at or
    // after maxExclusive, up to about maxBytes (always at least one batch if any qualifies).
    // The first batch may start before `from`; callers slice it (LLD §5.4, §6.6).
    Result<ReadResult> read(Offset from, Offset maxExclusive, std::size_t maxBytes) const;

    // Log end offset: the offset the next appended record will get.
    Offset leo() const { return leo_.load(std::memory_order_acquire); }
    Offset logStartOffset() const { return logStart_.load(std::memory_order_acquire); }

    // fdatasyncs every segment that may hold unflushed data, then advances the recovery
    // point to the LEO read before the sync (LLD §5.8). Called by the broker scheduler,
    // concurrently with append().
    Status flush();

    // Everything below this offset is known to be on disk; recovery scans from here.
    Offset recoveryPoint() const { return recoveryPoint_.load(std::memory_order_acquire); }
    std::size_t segmentCount() const;

private:
    PartitionLog(std::string dir, LogConfig cfg, std::shared_ptr<Segment> first);
    bool shouldRoll(const Segment& active, std::size_t batchBytes) const;
    Status roll();  // requires the append caller's exclusivity
    std::vector<std::shared_ptr<Segment>> snapshot() const;

    const std::string dir_;
    const LogConfig cfg_;
    mutable std::mutex segMu_;
    std::vector<std::shared_ptr<Segment>> segments_;  // by base offset; back() is active
    // LLD §11.4: stored with release after the batch is written; readers load with acquire
    // and read only below it, so they never see a partly written batch.
    std::atomic<Offset> leo_{0};
    std::atomic<Offset> logStart_{0};
    std::atomic<Offset> recoveryPoint_{0};
};

}  // namespace elog
