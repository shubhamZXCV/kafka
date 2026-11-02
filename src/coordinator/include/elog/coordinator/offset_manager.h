#pragma once

#include "elog/common/events.h"
#include "elog/common/metrics.h"
#include "elog/coordinator/repository.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/group_messages.h"

namespace elog {

// OffsetCommit and OffsetFetch (LLD §7.4). Committed offsets live only in SQLite, which is
// authoritative and survives restarts. With fencing on, a commit is stored only if the
// committer owns the partition at the epoch it sent (the §8.3 statement); there is
// deliberately no in-memory membership check, so fencing lives in exactly one place.
// coordinator.fencing.enabled=false exists only for the E5 control run.
//
// Thread rules: event-loop thread only (LLD §11.3).
class OffsetManager {
public:
    OffsetManager(Repository& repo, EventLog& events, Metrics& metrics, bool fencingEnabled);

    Frame onCommit(const Frame& request, TimeMs nowWallMs);
    Frame onFetch(const Frame& request);
    void onTick(TimeMs now);  // FENCING_DISABLED reminder

private:
    // One entry, inside the request's transaction; sets its per-partition error. A failed
    // Status means the database failed and the whole request is rolled back.
    Status commitEntry(const OffsetCommitRequest& r, const OffsetCommitEntry& e, TimeMs nowWallMs,
                       ErrorCode& error);

    Repository& repo_;
    EventLog& events_;
    Metrics& metrics_;
    const bool fencingEnabled_;
    TimeMs lastFencingWarningMs_ = 0;
};

}  // namespace elog
