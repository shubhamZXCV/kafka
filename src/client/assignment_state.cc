#include "elog/client/assignment_state.h"

namespace elog {

AssignmentSnapshot AssignmentState::snapshot() const {
    std::lock_guard lock(mu_);
    return {version_, owned_};
}

AssignmentDelta AssignmentState::apply(Generation generation,
                                       const std::vector<PartitionEpoch>& assignment) {
    std::lock_guard lock(mu_);
    ++version_;
    generation_ = generation;
    AssignmentDelta delta;
    std::map<TopicPartition, OwnedPartition> next;
    for (const PartitionEpoch& p : assignment) {
        const TopicPartition tp{p.topic, p.partition};
        const auto old = owned_.find(tp);
        if (old != owned_.end() && old->second.epoch == p.epoch) {
            next[tp] = old->second;  // retained: positions and version unchanged
            delta.retained.push_back(tp);
        } else {
            next[tp] = OwnedPartition{p.epoch, version_, std::nullopt, 0, std::nullopt};
            delta.added.push_back(tp);
        }
    }
    for (const auto& [tp, owned] : owned_) {
        const auto kept = next.find(tp);
        if (kept == next.end() || kept->second.assignedVersion == version_) {
            delta.revoked.push_back(tp);  // gone, or re-added under a new epoch
        }
    }
    owned_ = std::move(next);
    return delta;
}

void AssignmentState::revokeAll() {
    std::lock_guard lock(mu_);
    ++version_;
    owned_.clear();
}

bool AssignmentState::setFetchPosition(const TopicPartition& tp, std::uint64_t assignedVersion,
                                       Offset position) {
    std::lock_guard lock(mu_);
    const auto it = owned_.find(tp);
    if (it == owned_.end() || it->second.assignedVersion != assignedVersion) {
        return false;
    }
    it->second.fetchPosition = position;
    return true;
}

void AssignmentState::startAt(const TopicPartition& tp, Offset position, bool committed) {
    std::lock_guard lock(mu_);
    if (const auto it = owned_.find(tp); it != owned_.end()) {
        it->second.fetchPosition = position;
        it->second.processedNext = position;
        if (committed) {
            it->second.lastCommitted = position;
        }
    }
}

bool AssignmentState::owns(const TopicPartition& tp, std::uint64_t assignedVersion) const {
    std::lock_guard lock(mu_);
    const auto it = owned_.find(tp);
    return it != owned_.end() && it->second.assignedVersion == assignedVersion;
}

void AssignmentState::markProcessed(const TopicPartition& tp, Offset next) {
    std::lock_guard lock(mu_);
    if (const auto it = owned_.find(tp); it != owned_.end()) {
        it->second.processedNext = next;
    }
}

Generation AssignmentState::generation() const {
    std::lock_guard lock(mu_);
    return generation_;
}

}  // namespace elog
