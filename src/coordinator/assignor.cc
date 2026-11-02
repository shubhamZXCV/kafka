#include "elog/coordinator/assignor.h"

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

std::unique_ptr<Assignor> makeAssignor(std::string_view name) {
    if (name == "roundrobin") {
        return std::make_unique<RoundRobinAssignor>();
    }
    return nullptr;
}

}  // namespace elog
