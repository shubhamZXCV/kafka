#include "elog/storage/partition_log.h"

#include <algorithm>
#include <filesystem>

#include "elog/common/time.h"
#include "elog/protocol/record_batch.h"

namespace elog {

PartitionLog::PartitionLog(std::string dir, LogConfig cfg, std::shared_ptr<Segment> first)
    : dir_(std::move(dir)), cfg_(cfg) {
    logStart_.store(first->baseOffset(), std::memory_order_release);
    segments_.push_back(std::move(first));
}

Result<std::unique_ptr<PartitionLog>> PartitionLog::open(const std::string& dir, LogConfig cfg) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, "create log dir " + dir + ": " + ec.message()};
    }
    auto segment = Segment::create(dir, /*baseOffset=*/0, cfg.indexIntervalBytes);
    if (!segment.ok()) {
        return {segment.error(), segment.message()};
    }
    return std::unique_ptr<PartitionLog>(new PartitionLog(dir, cfg, std::move(segment).value()));
}

Result<AppendInfo> PartitionLog::append(std::span<std::byte> batchBytes, Epoch epoch,
                                        bool assignOffsets) {
    // Parse: CRC and record checks before anything reaches the disk (FR-LOG-07).
    auto view = RecordBatchView::parse(batchBytes);
    if (!view.ok()) {
        return {view.error(), view.message()};
    }

    // Validate / assign offsets. Only append() writes leo_, so relaxed is enough here.
    const Offset leo = leo_.load(std::memory_order_relaxed);
    if (assignOffsets) {
        stampBatch(batchBytes, leo, epoch);  // the view now reports the stamped values
    } else if (view.value().baseOffset() != leo) {
        return {ErrorCode::INVALID_REQUEST, "follower batch starts at " +
                                                std::to_string(view.value().baseOffset()) +
                                                " but log end is " + std::to_string(leo)};
    }

    // Only append() changes segments_, so reading back() without segMu_ is safe here.
    if (shouldRoll(*segments_.back(), batchBytes.size())) {
        if (auto status = roll(); !status.ok()) {
            return {status.error(), status.message()};
        }
    }
    Segment& active = *segments_.back();
    const auto relative =
        static_cast<std::uint32_t>(view.value().baseOffset() - active.baseOffset());
    const auto size = static_cast<std::uint32_t>(batchBytes.size());
    if (auto status = active.index().maybeAppend(relative, active.size(), size); !status.ok()) {
        return {status.error(), status.message()};
    }
    if (auto status = active.append(batchBytes); !status.ok()) {
        return {status.error(), status.message()};
    }
    const AppendInfo info{view.value().baseOffset(), view.value().lastOffset()};
    leo_.store(info.lastOffset + 1, std::memory_order_release);
    return info;
}

// LLD §5.3: never roll an empty segment (one oversized batch still gets a segment of its own).
bool PartitionLog::shouldRoll(const Segment& active, std::size_t batchBytes) const {
    return active.size() > 0 &&
           (active.size() + batchBytes > cfg_.segmentBytes ||
            time::monotonicMs() - active.createdMs() >= cfg_.segmentMs.count());
}

Status PartitionLog::roll() {
    auto segment =
        Segment::create(dir_, leo_.load(std::memory_order_relaxed), cfg_.indexIntervalBytes);
    if (!segment.ok()) {
        return {segment.error(), segment.message()};
    }
    std::lock_guard lock(segMu_);  // lock order: Replica::mu_ → PartitionLog::segMu_
    segments_.push_back(std::move(segment).value());
    return okStatus();
}

std::vector<std::shared_ptr<Segment>> PartitionLog::snapshot() const {
    std::lock_guard lock(segMu_);  // lock order: PartitionLog::segMu_ (LLD §11.2)
    return segments_;
}

std::size_t PartitionLog::segmentCount() const {
    return snapshot().size();
}

// LLD §5.4: find the segment holding `from`, jump to its index floor, then read forward
// across segments. No lock is held during I/O; the snapshot keeps the files open.
Result<ReadResult> PartitionLog::read(Offset from, Offset maxExclusive,
                                      std::size_t maxBytes) const {
    const Offset leo = this->leo();
    if (from < logStartOffset() || from > leo) {
        return {ErrorCode::OFFSET_OUT_OF_RANGE, "offset " + std::to_string(from) + " outside [" +
                                                    std::to_string(logStartOffset()) + ", " +
                                                    std::to_string(leo) + "]"};
    }
    maxExclusive = std::min(maxExclusive, leo);  // never past what leo_ has published
    const auto segments = snapshot();
    auto seg = std::upper_bound(
        segments.begin(), segments.end(), from,
        [](Offset target, const std::shared_ptr<Segment>& s) { return target < s->baseOffset(); });
    seg = seg == segments.begin() ? seg : std::prev(seg);
    std::uint64_t pos =
        (*seg)->index().floor(static_cast<std::uint32_t>(from - (*seg)->baseOffset())).position;

    ReadResult result;
    std::vector<std::byte> header;
    std::vector<std::byte> batch;
    for (; seg != segments.end(); ++seg, pos = 0) {
        const std::uint32_t segmentSize = (*seg)->size();
        while (pos < segmentSize) {
            if (auto status = (*seg)->readAt(pos, batch::kHeaderBytes, header); !status.ok()) {
                return {status.error(), status.message()};
            }
            const RecordBatchHeader h = readBatchHeader(header);
            if (h.baseOffset >= maxExclusive) {
                return result;
            }
            const std::size_t total = batch::kRecordBatchOverhead + h.batchLength;
            if (h.baseOffset + h.lastOffsetDelta >= from) {
                if (!result.batches.empty() && result.batches.size() + total > maxBytes) {
                    return result;  // the first batch is always returned, even if larger
                }
                if (auto status = (*seg)->readAt(pos, total, batch); !status.ok()) {
                    return {status.error(), status.message()};
                }
                result.batches.insert(result.batches.end(), batch.begin(), batch.end());
            }
            pos += total;
        }
    }
    return result;
}

}  // namespace elog
