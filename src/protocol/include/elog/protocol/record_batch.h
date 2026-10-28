#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "elog/common/types.h"

namespace elog {

// A record batch is the unit of produce, storage and fetch. The same bytes are used on the
// wire and on disk, so the leader appends what it received after stamping two fields
// (LLD §4.5):
//
//     offset size field             written by
//     0      8    baseOffset        leader (producer sends 0)
//     8      4    batchLength       producer: bytes after this field
//     12     4    leaderEpoch       leader (producer sends 0)
//     16     1    magic = 1         producer
//     17     4    crc32c            producer: CRC over bytes [21, end)
//     21     2    attributes = 0    producer
//     23     4    lastOffsetDelta   producer: recordCount - 1
//     27     8    firstTimestampMs  producer
//     35     8    maxTimestampMs    producer
//     43     4    recordCount       producer: >= 1
//     47     ...  records           recordCount MessagePack records
//
// The CRC starts at byte 21, so stamping baseOffset and leaderEpoch never invalidates it.
namespace batch {

inline constexpr std::size_t kBaseOffsetAt = 0;
inline constexpr std::size_t kBatchLengthAt = 8;
inline constexpr std::size_t kLeaderEpochAt = 12;
inline constexpr std::size_t kMagicAt = 16;
inline constexpr std::size_t kCrcAt = 17;
inline constexpr std::size_t kAttributesAt = 21;
inline constexpr std::size_t kLastOffsetDeltaAt = 23;
inline constexpr std::size_t kFirstTimestampAt = 27;
inline constexpr std::size_t kMaxTimestampAt = 35;
inline constexpr std::size_t kRecordCountAt = 43;
inline constexpr std::size_t kRecordsAt = 47;

inline constexpr std::size_t kHeaderBytes = 47;
inline constexpr std::size_t kCrcStart = kAttributesAt;  // CRC covers [kCrcStart, end)
// LLD RECORD_BATCH_OVERHEAD: baseOffset + batchLength. Batch size = this + batchLength.
inline constexpr std::size_t kRecordBatchOverhead = 12;
inline constexpr std::uint8_t kMagic = 1;

}  // namespace batch

// The fixed header fields, decoded.
struct RecordBatchHeader {
    Offset baseOffset = 0;
    std::uint32_t batchLength = 0;
    Epoch leaderEpoch = 0;
    std::uint8_t magic = batch::kMagic;
    std::uint32_t crc = 0;
    std::uint16_t attributes = 0;
    std::uint32_t lastOffsetDelta = 0;
    TimeMs firstTimestampMs = 0;
    TimeMs maxTimestampMs = 0;
    std::uint32_t recordCount = 0;
};

// Writes all header fields into out[0, kHeaderBytes). out must be at least that long.
void writeBatchHeader(std::span<std::byte> out, const RecordBatchHeader& header);

// Reads all header fields from in[0, kHeaderBytes). in must be at least that long; the
// values are not validated here.
RecordBatchHeader readBatchHeader(std::span<const std::byte> in);

// Collects records and encodes them as one batch (LLD §4.5). Used by the producer's
// accumulator, and by sliceBatch to re-encode part of a batch.
//
//     RecordBatchBuilder builder;
//     builder.add(key, value, time::wallMs());
//     std::vector<std::byte> bytes = builder.build();   // baseOffset and leaderEpoch are 0
//
// Not thread-safe: one owner at a time.
class RecordBatchBuilder {
public:
    // Encodes the record now; its offsetDelta is its position in the batch. key may be absent.
    void add(std::optional<std::span<const std::byte>> key, std::span<const std::byte> value,
             TimeMs timestampMs);

    // Size of the batch build() would return.
    std::size_t estimatedBytes() const { return batch::kHeaderBytes + records_.size(); }
    std::uint32_t count() const { return count_; }

    // The finished batch with its CRC. Requires count() >= 1.
    std::vector<std::byte> build() const;

private:
    std::vector<std::byte> records_;  // the encoded records, back to back
    std::uint32_t count_ = 0;
    TimeMs firstTimestampMs_ = 0;
    TimeMs maxTimestampMs_ = 0;
};

}  // namespace elog
