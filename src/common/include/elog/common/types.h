#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace elog {

// LLD §1.2: offsets are 64-bit, epochs and generations 32-bit, broker IDs signed.
using Offset = std::uint64_t;
using Epoch = std::uint32_t;  // leader epoch or ownership epoch
using Generation = std::uint32_t;
using BrokerId = std::int32_t;
using MemberId = std::string;
using TimeMs = std::int64_t;  // milliseconds; monotonic or wall clock, see time.h

struct TopicPartition {
    std::string topic;
    std::uint32_t partition = 0;

    auto operator<=>(const TopicPartition&) const = default;
};

}  // namespace elog

template <>
struct std::hash<elog::TopicPartition> {
    std::size_t operator()(const elog::TopicPartition& tp) const noexcept {
        return std::hash<std::string>{}(tp.topic) * 31 + tp.partition;
    }
};
