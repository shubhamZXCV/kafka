#pragma once

#include <chrono>

#include "elog/common/types.h"

// LLD §3.2. Rule: timeouts and deadlines always use monotonicMs(), never wall time, so a
// clock adjustment cannot cause false evictions.
namespace elog::time {

// For timeouts, deadlines and durations.
inline TimeMs monotonicMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

// For record and metric timestamps only.
inline TimeMs wallMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

}  // namespace elog::time
