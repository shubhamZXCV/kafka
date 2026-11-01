#pragma once

#include <pthread.h>
#include <signal.h>

#include <atomic>
#include <thread>

namespace elog::tools {

// Turns SIGINT/SIGTERM into a flag the tool's loops poll. The signals are blocked in every
// thread (create this before any other thread) and taken by one waiting thread, so no
// code runs in a signal handler. SIGUSR1 only wakes that thread on destruction.
class StopSignal {
public:
    StopSignal() {
        sigemptyset(&signals_);
        sigaddset(&signals_, SIGINT);
        sigaddset(&signals_, SIGTERM);
        sigaddset(&signals_, SIGUSR1);
        pthread_sigmask(SIG_BLOCK, &signals_, nullptr);
        waiter_ = std::thread([this] {
            int signal = 0;
            sigwait(&signals_, &signal);
            stopped_ = true;
        });
    }
    ~StopSignal() {
        pthread_kill(waiter_.native_handle(), SIGUSR1);
        waiter_.join();
    }
    StopSignal(const StopSignal&) = delete;
    StopSignal& operator=(const StopSignal&) = delete;

    bool stopped() const { return stopped_; }

private:
    sigset_t signals_;
    std::atomic<bool> stopped_{false};
    std::thread waiter_;
};

}  // namespace elog::tools
