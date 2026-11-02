#pragma once

#include <chrono>
#include <cstdint>
#include <vector>

#include "elog/client/assignment_state.h"
#include "elog/client/rpc_connection.h"
#include "elog/common/result.h"
#include "elog/protocol/group_messages.h"

namespace elog {

// Commits the consumer's processed positions (LLD §10.6). A commit is due when
// commitIntervalRecords records were processed since the last one, or commitIntervalTime
// has passed (a negative interval disables the time trigger, as E8 needs).
//
// Thread rules: processing thread only.
class OffsetCommitter {
public:
    OffsetCommitter(const std::string& coordinator, std::string groupId,
                    std::uint32_t intervalRecords, std::chrono::milliseconds intervalTime);

    void onProcessed() { ++sinceLastCommit_; }
    bool due() const;

    // Commits every owned partition processed past its last commit, with its ownership
    // epoch, and records the accepted ones. Returns the per-partition errors (empty on
    // success), or a transport error.
    Result<std::vector<PartitionError>> commit(const MemberId& memberId, AssignmentState& state);

private:
    RpcConnection coordinator_;
    const std::string groupId_;
    const std::uint32_t intervalRecords_;
    const std::chrono::milliseconds intervalTime_;
    std::uint32_t sinceLastCommit_ = 0;
    std::chrono::steady_clock::time_point lastCommit_ = std::chrono::steady_clock::now();
};

}  // namespace elog
