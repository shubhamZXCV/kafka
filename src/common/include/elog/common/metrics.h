#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "elog/common/result.h"

namespace elog {

// Monotonically increasing count, e.g. produce_records_total. Lock-free; any thread.
class Counter {
public:
    void add(std::int64_t n = 1) { value_.fetch_add(n, std::memory_order_relaxed); }
    std::int64_t value() const { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<std::int64_t> value_{0};
};

// Current level of something, e.g. inflight_depth. Lock-free; any thread.
class Gauge {
public:
    void set(std::int64_t v) { value_.store(v, std::memory_order_relaxed); }
    std::int64_t value() const { return value_.load(std::memory_order_relaxed); }

private:
    std::atomic<std::int64_t> value_{0};
};

// Samples collected over one flush interval, written as p50/p95/p99 and then reset
// (LLD §13.2), e.g. e2e_latency_ms. Thread-safe.
class Histogram {
public:
    void record(double sample);
    // Returns this interval's samples and starts a new interval.
    std::vector<double> takeSamples();

private:
    std::mutex mu_;
    std::vector<double> samples_;
};

// Registry of one process's metrics, written to <dir>/metrics-<node>.csv every interval
// (LLD §13.1) by a background thread. Each row is
//
//     ts_ms,node,metric,labels,value
//
// where labels look like "topic=orders;partition=3". Histograms produce three rows per
// interval with an extra label "quantile=p50|p95|p99". process_rss_bytes is added
// automatically.
//
// Thread-safe. Look a metric up once and keep the reference; it stays valid for the
// registry's lifetime.
class Metrics {
public:
    static Result<std::unique_ptr<Metrics>> open(const std::string& dir, const std::string& node,
                                                 std::chrono::milliseconds flushInterval);
    ~Metrics();  // stops the flush thread and writes a final row set

    Counter& counter(const std::string& name, const std::string& labels = {});
    Gauge& gauge(const std::string& name, const std::string& labels = {});
    Histogram& histogram(const std::string& name, const std::string& labels = {});

    void flush();

private:
    using Key = std::pair<std::string, std::string>;  // (metric name, labels)

    Metrics(std::ofstream out, std::string node, std::chrono::milliseconds flushInterval);
    void flushLoop();

    const std::string node_;
    const std::chrono::milliseconds flushInterval_;

    std::mutex mu_;  // guards the maps and out_
    std::map<Key, std::unique_ptr<Counter>> counters_;
    std::map<Key, std::unique_ptr<Gauge>> gauges_;
    std::map<Key, std::unique_ptr<Histogram>> histograms_;
    std::ofstream out_;

    std::mutex stopMu_;
    std::condition_variable stopCv_;
    bool stopping_ = false;
    std::thread flusher_;  // last member: starts after everything above is initialised
};

}  // namespace elog
