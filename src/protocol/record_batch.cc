#include "elog/protocol/record_batch.h"

#include <cassert>

#include "elog/common/bytes.h"

namespace elog {

void writeBatchHeader(std::span<std::byte> out, const RecordBatchHeader& h) {
    assert(out.size() >= batch::kHeaderBytes);
    storeBE<std::uint64_t>(out, batch::kBaseOffsetAt, h.baseOffset);
    storeBE<std::uint32_t>(out, batch::kBatchLengthAt, h.batchLength);
    storeBE<std::uint32_t>(out, batch::kLeaderEpochAt, h.leaderEpoch);
    storeBE<std::uint8_t>(out, batch::kMagicAt, h.magic);
    storeBE<std::uint32_t>(out, batch::kCrcAt, h.crc);
    storeBE<std::uint16_t>(out, batch::kAttributesAt, h.attributes);
    storeBE<std::uint32_t>(out, batch::kLastOffsetDeltaAt, h.lastOffsetDelta);
    // Timestamps are signed; they travel as their two's-complement bit pattern.
    storeBE<std::uint64_t>(out, batch::kFirstTimestampAt,
                           static_cast<std::uint64_t>(h.firstTimestampMs));
    storeBE<std::uint64_t>(out, batch::kMaxTimestampAt,
                           static_cast<std::uint64_t>(h.maxTimestampMs));
    storeBE<std::uint32_t>(out, batch::kRecordCountAt, h.recordCount);
}

RecordBatchHeader readBatchHeader(std::span<const std::byte> in) {
    assert(in.size() >= batch::kHeaderBytes);
    return RecordBatchHeader{
        .baseOffset = loadBE<std::uint64_t>(in, batch::kBaseOffsetAt),
        .batchLength = loadBE<std::uint32_t>(in, batch::kBatchLengthAt),
        .leaderEpoch = loadBE<std::uint32_t>(in, batch::kLeaderEpochAt),
        .magic = loadBE<std::uint8_t>(in, batch::kMagicAt),
        .crc = loadBE<std::uint32_t>(in, batch::kCrcAt),
        .attributes = loadBE<std::uint16_t>(in, batch::kAttributesAt),
        .lastOffsetDelta = loadBE<std::uint32_t>(in, batch::kLastOffsetDeltaAt),
        .firstTimestampMs =
            static_cast<TimeMs>(loadBE<std::uint64_t>(in, batch::kFirstTimestampAt)),
        .maxTimestampMs = static_cast<TimeMs>(loadBE<std::uint64_t>(in, batch::kMaxTimestampAt)),
        .recordCount = loadBE<std::uint32_t>(in, batch::kRecordCountAt),
    };
}

}  // namespace elog
