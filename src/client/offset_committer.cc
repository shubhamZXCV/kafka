#include "elog/client/offset_committer.h"

#include "elog/protocol/group_codec.h"

namespace elog {

namespace {

std::pair<std::string, std::uint16_t> address(const std::string& coordinator) {
    auto parsed = parseHostPort(coordinator);
    return parsed.ok() ? parsed.value() : std::pair<std::string, std::uint16_t>{"", 0};
}

}  // namespace

OffsetCommitter::OffsetCommitter(const std::string& coordinator, std::string groupId,
                                 std::uint32_t intervalRecords,
                                 std::chrono::milliseconds intervalTime)
    : coordinator_(address(coordinator).first, address(coordinator).second),
      groupId_(std::move(groupId)),
      intervalRecords_(intervalRecords),
      intervalTime_(intervalTime) {}

bool OffsetCommitter::due() const {
    if (sinceLastCommit_ == 0) {
        return false;
    }
    return sinceLastCommit_ >= intervalRecords_ ||
           (intervalTime_.count() >= 0 &&
            std::chrono::steady_clock::now() - lastCommit_ >= intervalTime_);
}

Result<std::vector<PartitionError>> OffsetCommitter::commit(const MemberId& memberId,
                                                            AssignmentState& state) {
    const std::vector<OffsetCommitEntry> entries = state.pendingCommits();
    sinceLastCommit_ = 0;
    lastCommit_ = std::chrono::steady_clock::now();
    if (entries.empty()) {
        return std::vector<PartitionError>{};
    }
    auto frame = coordinator_.call(encode(OffsetCommitRequest{groupId_, memberId, entries}, 0));
    if (!frame.ok()) {
        return {frame.error(), frame.message()};
    }
    auto response = decodeOffsetCommitResponse(frame.value());
    if (!response.ok() || response.value().offsets.size() != entries.size()) {
        return {ErrorCode::CORRUPT_MESSAGE, "bad OffsetCommit response"};
    }
    std::vector<PartitionError> errors;
    for (std::size_t i = 0; i < entries.size(); ++i) {
        const OffsetCommitEntry& e = entries[i];
        if (response.value().offsets[i].error == ErrorCode::NONE) {
            state.markCommitted({e.topic, e.partition}, e.epoch, e.offset);
        } else {
            errors.push_back(response.value().offsets[i]);
        }
    }
    return errors;
}

Result<std::vector<OffsetFetchEntry>> OffsetCommitter::fetchCommitted(
    const std::vector<TopicPartition>& partitions) {
    auto frame = coordinator_.call(encode(OffsetFetchRequest{groupId_, partitions}, 0));
    if (!frame.ok()) {
        return {frame.error(), frame.message()};
    }
    auto response = decodeOffsetFetchResponse(frame.value());
    if (!response.ok()) {
        return {response.error(), response.message()};
    }
    return response.value().offsets;
}

}  // namespace elog
