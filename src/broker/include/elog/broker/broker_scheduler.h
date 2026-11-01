#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace elog {

// Runs the broker's periodic jobs on one thread (LLD §6.10): flush, checkpoints, and later
// retention and ISR checks. A job that is slow delays the others, which is fine at these
// intervals (seconds).
//
// Thread rules: every() is called before start(); start() and stop() by the server thread.
// Jobs run on the scheduler thread and must log their own errors.
class BrokerScheduler {
public:
    void every(std::string name, std::chrono::milliseconds interval, std::function<void()> job);
    void start();
    void stop();  // runs no further jobs; waits for the current one

private:
    using Clock = std::chrono::steady_clock;
    struct Job {
        std::string name;
        std::chrono::milliseconds interval;
        std::function<void()> run;
        Clock::time_point next;
    };

    void loop();

    std::vector<Job> jobs_;
    std::mutex mu_;  // guards stopping_ only
    std::condition_variable cv_;
    bool stopping_ = false;
    std::thread thread_;
};

}  // namespace elog
