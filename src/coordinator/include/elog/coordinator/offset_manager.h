#pragma once

#include "elog/common/events.h"
#include "elog/coordinator/repository.h"
#include "elog/protocol/frame.h"

namespace elog {

// OffsetCommit and OffsetFetch (LLD §7.4). Committed offsets live only in SQLite, which is
// authoritative and survives restarts.
//
// Thread rules: event-loop thread only (LLD §11.3).
class OffsetManager {
public:
    OffsetManager(Repository& repo, EventLog& events);

    Frame onCommit(const Frame& request, TimeMs nowWallMs);
    Frame onFetch(const Frame& request);

private:
    Repository& repo_;
    EventLog& events_;
};

}  // namespace elog
