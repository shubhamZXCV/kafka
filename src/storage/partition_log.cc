#include "elog/storage/partition_log.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <system_error>

#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/record_batch.h"

namespace elog {

namespace {

constexpr std::size_t kMaxSegmentsSyncedOneByOne = 4;

}  // namespace

PartitionLog::PartitionLog(std::string dir, LogConfig cfg,
                           std::vector<std::shared_ptr<Segment>> segments, Offset leo)
    : dir_(std::move(dir)), cfg_(cfg), segments_(std::move(segments)) {
    logStart_.store(segments_.front()->baseOffset(), std::memory_order_release);
    leo_.store(leo, std::memory_order_release);
    recoveryPoint_.store(leo, std::memory_order_release);  // everything left was verified
}

Result<std::unique_ptr<PartitionLog>> PartitionLog::open(const std::string& dir, LogConfig cfg,
                                                         Offset recoveryPoint) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, "create log dir " + dir + ": " + ec.message()};
    }
    auto segments = openSegments(dir, cfg);
    if (!segments.ok()) {
        return {segments.error(), segments.message()};
    }
    auto leo = recover(segments.value(), recoveryPoint);
    if (!leo.ok()) {
        return {leo.error(), leo.message()};
    }
    return std::unique_ptr<PartitionLog>(
        new PartitionLog(dir, cfg, std::move(segments).value(), leo.value()));
}

// The .log files of `dir` in offset order, or a new segment 0 if there are none.
Result<std::vector<std::shared_ptr<Segment>>> PartitionLog::openSegments(const std::string& dir,
                                                                         const LogConfig& cfg) {
    // LLD §5.6, in order: drop leftovers of interrupted writes, then find the segments.
    std::vector<Offset> bases;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        const auto& path = entry.path();
        const auto name = path.filename().string();
        const bool orphanIndex =
            path.extension() == ".index" &&
            !std::filesystem::exists(std::filesystem::path(path).replace_extension(".log"));
        if (path.extension() == ".tmp" || orphanIndex) {
            logging::get("storage")->warn("orphan_file_removed file={}", path.string());
            std::filesystem::remove(path);
        } else if (path.extension() == ".log" && name.size() == 24) {
            bases.push_back(std::stoull(name.substr(0, 20)));
        }
    }
    std::sort(bases.begin(), bases.end());
    std::vector<std::shared_ptr<Segment>> segments;
    for (Offset base : bases) {
        auto segment = Segment::open(dir, base, cfg.indexIntervalBytes);
        if (!segment.ok()) {
            return {segment.error(), segment.message()};
        }
        segments.push_back(std::move(segment).value());
    }
    if (segments.empty()) {
        auto segment = Segment::create(dir, 0, cfg.indexIntervalBytes);
        if (!segment.ok()) {
            return {segment.error(), segment.message()};
        }
        segments.push_back(std::move(segment).value());
    }
    return segments;
}

// LLD §5.6: scan the segments that may hold unflushed batches. The first batch that is cut
// short, has an impossible length, or fails its CRC/magic check marks the torn tail: the
// segment is truncated there and every later segment deleted. Returns the log end offset.
Result<Offset> PartitionLog::recover(std::vector<std::shared_ptr<Segment>>& segments,
                                     Offset recoveryPoint) {
    std::size_t first = 0;  // the segment holding the recovery point
    while (first + 1 < segments.size() && segments[first + 1]->baseOffset() <= recoveryPoint) {
        ++first;
    }
    Offset leo = 0;
    std::vector<std::byte> header, batch;
    for (std::size_t i = first; i < segments.size(); ++i) {
        Segment& seg = *segments[i];
        leo = seg.baseOffset();  // an empty segment ends where it starts
        std::uint32_t pos = 0;
        while (seg.size() - pos >= batch::kHeaderBytes) {
            if (!seg.readAt(pos, batch::kHeaderBytes, header).ok()) {
                break;
            }
            const std::uint64_t total =
                batch::kRecordBatchOverhead + readBatchHeader(header).batchLength;
            if (total > seg.size() - pos || !seg.readAt(pos, total, batch).ok()) {
                break;
            }
            auto view = RecordBatchView::parse(batch);
            if (!view.ok()) {
                break;
            }
            leo = view.value().lastOffset() + 1;
            pos += static_cast<std::uint32_t>(total);
        }
        if (pos < seg.size()) {
            logging::get("storage")->warn("torn_tail_truncated segment={} position={} size={}",
                                          seg.baseOffset(), pos, seg.size());
            for (std::size_t later = i + 1; later < segments.size(); ++later) {
                std::filesystem::remove(segments[later]->path());
                std::filesystem::remove(segments[later]->indexPath());
            }
            segments.resize(i + 1);
            if (auto status = seg.truncateToPos(pos); !status.ok()) {
                return {status.error(), status.message()};
            }
            if (auto status = seg.index().truncateAfterPos(pos); !status.ok()) {
                return {status.error(), status.message()};
            }
            break;
        }
    }
    return leo;
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
    if (cfg_.flushPolicy == FlushPolicy::BATCH) {
        if (auto status = flush(); !status.ok()) {
            return {status.error(), status.message()};
        }
    }
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

Status PartitionLog::flush() {
    const Offset leo = this->leo();  // what this flush covers
    const Offset from = recoveryPoint();
    std::vector<std::shared_ptr<Segment>> dirty;
    const auto segments = snapshot();
    for (std::size_t i = 0; i < segments.size(); ++i) {
        // Segment i holds offsets [base_i, base_i+1): it has nothing unflushed if the next
        // segment already starts at or below the recovery point.
        if (i + 1 == segments.size() || segments[i + 1]->baseOffset() > from) {
            dirty.push_back(segments[i]);
        }
    }
    if (auto status = syncSegments(dirty); !status.ok()) {
        return status;
    }
    // LLD §5.8: advance only after every sync succeeded.
    recoveryPoint_.store(std::max(from, leo), std::memory_order_release);
    return okStatus();
}

// One fdatasync per file costs a journal commit each; after a burst with small segments
// that is hundreds of commits. syncfs() makes every dirty file of the filesystem durable in
// one, so it is used once more than a few segments are dirty.
Status PartitionLog::syncSegments(const std::vector<std::shared_ptr<Segment>>& dirty) const {
    if (dirty.size() > kMaxSegmentsSyncedOneByOne) {
        const int fd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        const int rc = fd < 0 ? -1 : ::syncfs(fd);
        const int err = errno;
        if (fd >= 0) {
            ::close(fd);
        }
        return rc == 0 ? okStatus()
                       : Status{ErrorCode::UNKNOWN_SERVER_ERROR,
                                "syncfs " + dir_ + ": " + std::system_category().message(err)};
    }
    for (const auto& segment : dirty) {
        if (auto status = segment->flush(); !status.ok()) {
            return status;
        }
        if (auto status = segment->index().flush(); !status.ok()) {
            return status;
        }
    }
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
