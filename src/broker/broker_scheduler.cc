#include "elog/broker/broker_scheduler.h"

#include <algorithm>
#include <utility>

namespace elog {

void BrokerScheduler::every(std::string name, std::chrono::milliseconds interval,
                            std::function<void()> job) {
    jobs_.push_back({std::move(name), interval, std::move(job), Clock::now() + interval});
}

void BrokerScheduler::start() {
    thread_ = std::thread([this] { loop(); });
}

void BrokerScheduler::stop() {
    {
        std::lock_guard lock(mu_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void BrokerScheduler::loop() {
    while (true) {
        auto next = Clock::now() + std::chrono::hours(1);
        for (const Job& job : jobs_) {
            next = std::min(next, job.next);
        }
        {
            std::unique_lock lock(mu_);
            if (cv_.wait_until(lock, next, [&] { return stopping_; })) {
                return;
            }
        }
        for (Job& job : jobs_) {
            if (Clock::now() >= job.next) {
                job.run();
                job.next = Clock::now() + job.interval;
            }
        }
    }
}

}  // namespace elog
