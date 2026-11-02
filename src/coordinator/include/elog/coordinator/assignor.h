#pragma once

#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "elog/common/types.h"

namespace elog {

using Assignment = std::map<TopicPartition, MemberId>;

// A partition assignment strategy (LLD §9.1). All members subscribe to the same topics.
class Assignor {
public:
    virtual ~Assignor() = default;
    virtual std::string_view name() const = 0;
    // true: every partition gets a new ownership epoch on every rebalance.
    virtual bool eager() const = 0;
    // Pure and deterministic. members: sorted, non-empty. partitions: sorted.
    // previous: only entries whose owner is in `members`.
    virtual Assignment assign(std::span<const MemberId> members,
                              std::span<const TopicPartition> partitions,
                              const Assignment& previous) const = 0;
};

// LLD §9.2: partition i goes to member i mod m. Balanced (counts differ by at most one),
// ignores `previous`, and is eager.
class RoundRobinAssignor final : public Assignor {
public:
    std::string_view name() const override { return "roundrobin"; }
    bool eager() const override { return true; }
    Assignment assign(std::span<const MemberId> members, std::span<const TopicPartition> partitions,
                      const Assignment& previous) const override;
};

// The assignor called `name`, or nullptr if there is none.
std::unique_ptr<Assignor> makeAssignor(std::string_view name);

}  // namespace elog
