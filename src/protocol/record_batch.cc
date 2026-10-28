#include "elog/protocol/record_batch.h"

#include <algorithm>
#include <cassert>

#include <msgpack.hpp>

#include "elog/common/bytes.h"
#include "elog/common/crc32c.h"

namespace elog {

namespace {

// Lets msgpack::packer append straight into a byte vector.
struct ByteSink {
    std::vector<std::byte>& out;

    void write(const char* data, std::size_t size) {
        const auto* bytes = reinterpret_cast<const std::byte*>(data);
        out.insert(out.end(), bytes, bytes + size);
    }
};

void packBin(msgpack::packer<ByteSink>& packer, std::span<const std::byte> bytes) {
    packer.pack_bin(static_cast<std::uint32_t>(bytes.size()));
    packer.pack_bin_body(reinterpret_cast<const char*>(bytes.data()),
                         static_cast<std::uint32_t>(bytes.size()));
}

}  // namespace

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

void RecordBatchBuilder::add(std::optional<std::span<const std::byte>> key,
                             std::span<const std::byte> value, TimeMs timestampMs) {
    if (count_ == 0) {
        firstTimestampMs_ = timestampMs;
        maxTimestampMs_ = timestampMs;
    }
    maxTimestampMs_ = std::max(maxTimestampMs_, timestampMs);

    // LLD §4.5: [offsetDelta: uint, timestampDelta: int, key: bin | nil, value: bin]
    ByteSink sink{records_};
    msgpack::packer<ByteSink> packer(sink);
    packer.pack_array(4);
    packer.pack_uint32(count_);
    packer.pack_int64(timestampMs - firstTimestampMs_);
    if (key) {
        packBin(packer, *key);
    } else {
        packer.pack_nil();
    }
    packBin(packer, value);
    ++count_;
}

std::vector<std::byte> RecordBatchBuilder::build() const {
    assert(count_ > 0 && "a batch holds at least one record");
    std::vector<std::byte> out(batch::kHeaderBytes + records_.size());
    std::copy(records_.begin(), records_.end(), out.begin() + batch::kRecordsAt);

    writeBatchHeader(
        out,
        RecordBatchHeader{
            .baseOffset = 0,  // stamped by the leader
            .batchLength = static_cast<std::uint32_t>(out.size() - batch::kRecordBatchOverhead),
            .leaderEpoch = 0,  // stamped by the leader
            .magic = batch::kMagic,
            .crc = 0,  // filled in below
            .attributes = 0,
            .lastOffsetDelta = count_ - 1,
            .firstTimestampMs = firstTimestampMs_,
            .maxTimestampMs = maxTimestampMs_,
            .recordCount = count_,
        });
    // The CRC covers attributes through the last record, so it is computed last.
    storeBE<std::uint32_t>(out, batch::kCrcAt, crc32c(std::span(out).subspan(batch::kCrcStart)));
    return out;
}

}  // namespace elog
