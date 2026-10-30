#include "elog/storage/partition_log.h"

#include <algorithm>
#include <filesystem>

#include "elog/protocol/record_batch.h"

namespace elog {

Result<std::unique_ptr<PartitionLog>> PartitionLog::open(const std::string& dir) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, "create log dir " + dir + ": " + ec.message()};
    }
    auto segment = Segment::create(dir, /*baseOffset=*/0);
    if (!segment.ok()) {
        return {segment.error(), segment.message()};
    }
    return std::unique_ptr<PartitionLog>(new PartitionLog(dir, std::move(segment).value()));
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

    // Write, then publish.
    if (auto status = segment_->append(batchBytes); !status.ok()) {
        return {status.error(), status.message()};
    }
    const AppendInfo info{view.value().baseOffset(), view.value().lastOffset()};
    leo_.store(info.lastOffset + 1, std::memory_order_release);
    return info;
}

Result<ReadResult> PartitionLog::read(Offset from, Offset maxExclusive,
                                      std::size_t maxBytes) const {
    const Offset leo = this->leo();
    if (from < logStartOffset() || from > leo) {
        return {ErrorCode::OFFSET_OUT_OF_RANGE, "offset " + std::to_string(from) + " outside [" +
                                                    std::to_string(logStartOffset()) + ", " +
                                                    std::to_string(leo) + "]"};
    }
    // Never read past what leo_ has published (LLD §5.4).
    maxExclusive = std::min(maxExclusive, leo);

    // No index yet: walk batch headers from the start of the segment.
    ReadResult result;
    std::vector<std::byte> header;
    std::vector<std::byte> batch;
    const std::uint32_t segmentSize = segment_->size();
    std::uint64_t pos = 0;
    while (pos < segmentSize) {
        if (auto status = segment_->readAt(pos, batch::kHeaderBytes, header); !status.ok()) {
            return {status.error(), status.message()};
        }
        const RecordBatchHeader h = readBatchHeader(header);
        if (h.baseOffset >= maxExclusive) {
            break;
        }
        const std::size_t total = batch::kRecordBatchOverhead + h.batchLength;
        const Offset last = h.baseOffset + h.lastOffsetDelta;
        if (last >= from) {
            if (!result.batches.empty() && result.batches.size() + total > maxBytes) {
                break;  // the first batch is always returned, even if larger than maxBytes
            }
            if (auto status = segment_->readAt(pos, total, batch); !status.ok()) {
                return {status.error(), status.message()};
            }
            result.batches.insert(result.batches.end(), batch.begin(), batch.end());
        }
        pos += total;
    }
    return result;
}

}  // namespace elog
