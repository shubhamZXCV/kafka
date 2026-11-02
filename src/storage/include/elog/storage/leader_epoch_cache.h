#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "elog/common/result.h"
#include "elog/common/types.h"

namespace elog {

struct EpochEntry {
    Epoch epoch = 0;
    Offset startOffset = 0;  // first offset written in this epoch
};

// Which leader epoch wrote which part of the log (LLD §5.1, ADR-10), persisted as
// <partition dir>/leader-epoch.checkpoint (LLD §5.2.3, atomic writes). Followers use it to
// find where their log diverges from the new leader's.
//
// Thread rules: one caller at a time (Replica::mu_ is held for every use).
class LeaderEpochCache {
public:
    // Loads the cache file, or starts empty if there is none.
    static Result<std::unique_ptr<LeaderEpochCache>> open(const std::string& dir);

    // Records that `epoch` starts at `startOffset`; a no-op if it is already the latest.
    Status assign(Epoch epoch, Offset startOffset);
    std::optional<Epoch> latestEpoch() const;

    // LLD §6.8, leader side: the largest epoch <= `epoch` and where it ends (the start of
    // the next epoch, or `leo` if it is the latest). With no such epoch: (epoch, first start).
    std::pair<Epoch, Offset> endOffsetFor(Epoch epoch, Offset leo) const;

    Status truncateFromEnd(Offset offset);    // drops epochs starting at or after offset
    Status truncateFromStart(Offset offset);  // retention: the log now starts at offset

    const std::vector<EpochEntry>& entries() const { return entries_; }

private:
    LeaderEpochCache(std::string path, std::vector<EpochEntry> entries)
        : path_(std::move(path)), entries_(std::move(entries)) {}
    Status persist() const;

    const std::string path_;
    std::vector<EpochEntry> entries_;  // strictly increasing epochs and start offsets
};

}  // namespace elog
