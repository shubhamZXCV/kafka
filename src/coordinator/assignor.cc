#include "elog/coordinator/assignor.h"

#include <algorithm>
#include <vector>

namespace elog {

Assignment RoundRobinAssignor::assign(std::span<const MemberId> members,
                                      std::span<const TopicPartition> partitions,
                                      const Assignment&) const {
    Assignment result;
    for (std::size_t i = 0; i < partitions.size(); ++i) {
        result[partitions[i]] = members[i % members.size()];
    }
    return result;
}

Assignment StickyAssignor::assign(std::span<const MemberId> members,
                                  std::span<const TopicPartition> partitions,
                                  const Assignment& previous) const {
    const std::size_t base = partitions.size() / members.size();
    const std::size_t extra = partitions.size() % members.size();
    std::map<MemberId, std::vector<TopicPartition>> retained;  // sorted, as partitions are
    for (const MemberId& member : members) {
        retained[member];
    }
    for (const TopicPartition& tp : partitions) {
        if (auto it = previous.find(tp); it != previous.end() && retained.contains(it->second)) {
            retained[it->second].push_back(tp);
        }
    }
    // The proof in §9.3: total retention is largest when the extra slots go to the members
    // with the most retained partitions. Ties keep member-ID order (deterministic).
    std::vector<MemberId> order(members.begin(), members.end());
    std::stable_sort(order.begin(), order.end(), [&](const MemberId& a, const MemberId& b) {
        return retained[a].size() > retained[b].size();
    });
    std::map<MemberId, std::size_t> quota;
    for (std::size_t i = 0; i < order.size(); ++i) {
        quota[order[i]] = base + (i < extra ? 1 : 0);
    }
    Assignment result;
    for (const MemberId& member : members) {
        const auto& owned = retained[member];
        for (std::size_t i = 0; i < std::min(owned.size(), quota[member]); ++i) {
            result[owned[i]] = member;
        }
    }
    std::vector<TopicPartition> free;  // over quota or unowned, in partition order
    for (const TopicPartition& tp : partitions) {
        if (!result.contains(tp)) {
            free.push_back(tp);
        }
    }
    auto next = free.begin();
    for (const MemberId& member : members) {
        for (std::size_t have = std::min(retained[member].size(), quota[member]);
             have < quota[member]; ++have) {
            result[*next++] = member;
        }
    }
    return result;
}

std::unique_ptr<Assignor> makeAssignor(std::string_view name) {
    if (name == "roundrobin") {
        return std::make_unique<RoundRobinAssignor>();
    }
    if (name == "sticky") {
        return std::make_unique<StickyAssignor>();
    }
    return nullptr;
}

}  // namespace elog
