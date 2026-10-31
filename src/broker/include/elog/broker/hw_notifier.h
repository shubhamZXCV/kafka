#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace elog {

// Broker-wide "some high watermark or role changed" signal for fetch long-polls (LLD §6.1,
// R-5). One fetch spans many partitions, so it waits on this versioned counter instead of
// on each partition. A waiter reads version() before looking at data, so a bump between its
// look and its wait is not missed.
//
// Thread rules: safe from any thread. mu_ is last in the lock order (LLD §11.2), so bump()
// may be called while holding Replica::mu_.
class HwNotifier {
public:
    using Deadline = std::chrono::steady_clock::time_point;

    void bump() {
        {
            std::lock_guard lock(mu_);  // lock order: Replica::mu_ → HwNotifier::mu_
            ++version_;
        }
        cv_.notify_all();
    }

    std::uint64_t version() const {
        std::lock_guard lock(mu_);  // lock order: HwNotifier::mu_ (LLD §11.2)
        return version_;
    }

    // Waits until the version differs from `seen` or the deadline passes; returns the
    // version now.
    std::uint64_t waitForChange(std::uint64_t seen, Deadline deadline) {
        std::unique_lock lock(mu_);  // lock order: HwNotifier::mu_ (LLD §11.2)
        cv_.wait_until(lock, deadline, [&] { return version_ != seen; });
        return version_;
    }

private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::uint64_t version_ = 0;
};

}  // namespace elog
