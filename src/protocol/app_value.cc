#include "elog/protocol/app_value.h"

#include <exception>
#include <string_view>

#include <msgpack.hpp>

namespace elog {

namespace {

struct ByteSink {
    std::vector<std::byte>& out;
    void write(const char* data, std::size_t size) {
        const auto* bytes = reinterpret_cast<const std::byte*>(data);
        out.insert(out.end(), bytes, bytes + size);
    }
};

}  // namespace

std::vector<std::byte> encodeAppValue(const AppValue& value) {
    std::vector<std::byte> out;
    ByteSink sink{out};
    msgpack::packer<ByteSink> packer(sink);
    packer.pack_map(4);
    packer.pack(std::string_view("pid"));
    packer.pack(value.pid);
    packer.pack(std::string_view("seq"));
    packer.pack(value.seq);
    packer.pack(std::string_view("ts"));
    packer.pack(value.ts);
    packer.pack(std::string_view("pad"));
    packer.pack_bin(static_cast<std::uint32_t>(value.padBytes));
    const std::vector<char> zeros(value.padBytes, 0);
    packer.pack_bin_body(zeros.data(), static_cast<std::uint32_t>(zeros.size()));
    return out;
}

// Values come from the log, so they are untrusted: any shape error is CORRUPT_MESSAGE.
Result<AppValue> decodeAppValue(std::span<const std::byte> bytes) {
    try {
        const auto handle =
            msgpack::unpack(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        const msgpack::object& root = handle.get();
        if (root.type != msgpack::type::MAP) {
            return {ErrorCode::CORRUPT_MESSAGE, "value is not a map"};
        }
        AppValue value;
        for (const auto& kv : std::span(root.via.map.ptr, root.via.map.size)) {
            const auto key = kv.key.as<std::string_view>();
            if (key == "pid") {
                value.pid = kv.val.as<std::string>();
            } else if (key == "seq") {
                value.seq = kv.val.as<std::uint64_t>();
            } else if (key == "ts") {
                value.ts = kv.val.as<TimeMs>();
            } else if (key == "pad" && kv.val.type == msgpack::type::BIN) {
                value.padBytes = kv.val.via.bin.size;
            }
        }
        return value;
    } catch (const std::exception& e) {
        return {ErrorCode::CORRUPT_MESSAGE, std::string("bad value: ") + e.what()};
    }
}

}  // namespace elog
