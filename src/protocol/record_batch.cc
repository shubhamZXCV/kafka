#include "elog/protocol/record_batch.h"

#include <algorithm>
#include <cassert>
#include <exception>
#include <string>

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

// Makes msgpack point bin objects at our buffer instead of copying them, so a Record's key
// and value are views into the batch bytes.
bool referenceBin(msgpack::type::object_type type, std::size_t /*size*/, void* /*userData*/) {
    return type == msgpack::type::BIN;
}

std::span<const std::byte> binBytes(const msgpack::object& o) {
    return std::as_bytes(std::span(o.via.bin.ptr, o.via.bin.size));
}

Status corrupt(const std::string& why) {
    return {ErrorCode::CORRUPT_MESSAGE, "record batch: " + why};
}

// Decodes the records section and checks its shape: exactly `count` records filling it
// completely, each [offsetDelta == index, timestampDelta, key: bin | nil, value: bin].
// Calls fn (if set) for each record. Used both to validate and to iterate.
Status decodeRecords(std::span<const std::byte> records, std::uint32_t count, Offset baseOffset,
                     TimeMs firstTimestampMs, const std::function<void(const Record&)>& fn) {
    const char* data = reinterpret_cast<const char*>(records.data());
    // Bounds what a hostile length prefix can make msgpack allocate: one flat 4-element
    // array per record, no maps, strings or extensions.
    const msgpack::unpack_limit limit(/*array=*/4, /*map=*/0, /*str=*/0,
                                      /*bin=*/records.size(), /*ext=*/0, /*depth=*/2);
    std::size_t pos = 0;
    try {
        for (std::uint32_t i = 0; i < count; ++i) {
            if (pos >= records.size()) {
                return corrupt("fewer records than recordCount");
            }
            const auto handle =
                msgpack::unpack(data, records.size(), pos, referenceBin, nullptr, limit);
            const msgpack::object& o = handle.get();
            if (o.type != msgpack::type::ARRAY || o.via.array.size != 4) {
                return corrupt("record is not a 4-element array");
            }
            const msgpack::object* f = o.via.array.ptr;
            const bool deltaOk = f[0].type == msgpack::type::POSITIVE_INTEGER && f[0].via.u64 == i;
            const bool tsOk = f[1].type == msgpack::type::POSITIVE_INTEGER ||
                              f[1].type == msgpack::type::NEGATIVE_INTEGER;
            const bool keyOk = f[2].type == msgpack::type::NIL || f[2].type == msgpack::type::BIN;
            const bool valueOk = f[3].type == msgpack::type::BIN;
            if (!deltaOk || !tsOk || !keyOk || !valueOk) {
                return corrupt("bad field in record " + std::to_string(i));
            }
            if (fn) {
                Record record;
                record.offset = baseOffset + i;
                record.timestampMs = firstTimestampMs + f[1].as<std::int64_t>();
                if (f[2].type == msgpack::type::BIN) {
                    record.key = binBytes(f[2]);
                }
                record.value = binBytes(f[3]);
                fn(record);
            }
        }
    } catch (const std::exception& e) {  // truncated or malformed MessagePack
        return corrupt(std::string("bad record encoding: ") + e.what());
    }
    if (pos != records.size()) {
        return corrupt("trailing bytes after the last record");
    }
    return okStatus();
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

Result<RecordBatchView> RecordBatchView::parse(std::span<const std::byte> bytes) {
    // Each check makes the next one safe: never read a field before its bytes are known
    // to exist.
    if (bytes.size() < batch::kHeaderBytes) {
        return {ErrorCode::CORRUPT_MESSAGE, "record batch: shorter than its header"};
    }
    const RecordBatchHeader h = readBatchHeader(bytes);
    if (batch::kRecordBatchOverhead + h.batchLength != bytes.size()) {
        return {ErrorCode::CORRUPT_MESSAGE, "record batch: batchLength does not match its size"};
    }
    if (h.magic != batch::kMagic) {
        return {ErrorCode::CORRUPT_MESSAGE, "record batch: bad magic"};
    }
    if (crc32c(bytes.subspan(batch::kCrcStart)) != h.crc) {
        return {ErrorCode::CORRUPT_MESSAGE, "record batch: CRC mismatch"};
    }
    if (h.attributes != 0) {
        return {ErrorCode::CORRUPT_MESSAGE, "record batch: reserved attribute bits set"};
    }
    if (h.recordCount == 0 || h.lastOffsetDelta != h.recordCount - 1) {
        return {ErrorCode::CORRUPT_MESSAGE, "record batch: bad recordCount or lastOffsetDelta"};
    }
    // A matching CRC only proves the bytes were not damaged after the producer wrote them;
    // the producer itself is untrusted, so the records are checked too.
    if (auto status = decodeRecords(bytes.subspan(batch::kRecordsAt), h.recordCount, h.baseOffset,
                                    h.firstTimestampMs, nullptr);
        !status.ok()) {
        return {status.error(), status.message()};
    }
    return RecordBatchView(bytes);
}

Offset RecordBatchView::baseOffset() const {
    return loadBE<std::uint64_t>(bytes_, batch::kBaseOffsetAt);
}

Offset RecordBatchView::lastOffset() const {
    return baseOffset() + loadBE<std::uint32_t>(bytes_, batch::kLastOffsetDeltaAt);
}

Epoch RecordBatchView::leaderEpoch() const {
    return loadBE<std::uint32_t>(bytes_, batch::kLeaderEpochAt);
}

std::uint32_t RecordBatchView::recordCount() const {
    return loadBE<std::uint32_t>(bytes_, batch::kRecordCountAt);
}

TimeMs RecordBatchView::maxTimestampMs() const {
    return static_cast<TimeMs>(loadBE<std::uint64_t>(bytes_, batch::kMaxTimestampAt));
}

void RecordBatchView::forEachRecord(const std::function<void(const Record&)>& fn) const {
    const RecordBatchHeader h = readBatchHeader(bytes_);
    const Status status = decodeRecords(bytes_.subspan(batch::kRecordsAt), h.recordCount,
                                        h.baseOffset, h.firstTimestampMs, fn);
    assert(status.ok() && "parse() already validated the records");
    (void)status;
}

}  // namespace elog
