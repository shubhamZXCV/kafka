#include "elog/common/metrics.h"

#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <format>

#include "elog/common/time.h"

namespace elog {

namespace {

// Nearest-rank quantile of sorted, non-empty samples.
double quantile(const std::vector<double>& sorted, double q) {
    const auto rank = static_cast<std::size_t>(std::ceil(q * static_cast<double>(sorted.size())));
    return sorted[std::max<std::size_t>(rank, 1) - 1];
}

std::string withLabel(const std::string& labels, const std::string& extra) {
    return labels.empty() ? extra : labels + ";" + extra;
}

// Resident set size from /proc/self/statm (second field, in pages). 0 if unavailable.
std::int64_t residentBytes() {
    std::ifstream statm("/proc/self/statm");
    std::int64_t sizePages = 0;
    std::int64_t residentPages = 0;
    if (!(statm >> sizePages >> residentPages)) {
        return 0;
    }
    return residentPages * sysconf(_SC_PAGESIZE);
}

template <class M>
M& findOrCreate(std::map<std::pair<std::string, std::string>, std::unique_ptr<M>>& map,
                const std::string& name, const std::string& labels) {
    auto& slot = map[{name, labels}];
    if (!slot) {
        slot = std::make_unique<M>();
    }
    return *slot;
}

}  // namespace

void Histogram::record(double sample) {
    std::lock_guard lock(mu_);
    samples_.push_back(sample);
}

std::vector<double> Histogram::takeSamples() {
    std::lock_guard lock(mu_);
    return std::exchange(samples_, {});
}

Result<std::unique_ptr<Metrics>> Metrics::open(const std::string& dir, const std::string& node,
                                               std::chrono::milliseconds flushInterval) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    const std::string path = dir + "/metrics-" + node + ".csv";
    // A restarted node appends to its old file; write the header only once.
    std::error_code sizeEc;
    const bool isNew = std::filesystem::file_size(path, sizeEc) == 0 || sizeEc;  // empty or missing
    std::ofstream out(path, std::ios::app);
    if (ec || !out) {
        return {ErrorCode::INVALID_REQUEST, "cannot open metrics file " + path};
    }
    if (isNew) {
        out << "ts_ms,node,metric,labels,value\n";
    }
    return std::unique_ptr<Metrics>(new Metrics(std::move(out), node, flushInterval));
}

Metrics::Metrics(std::ofstream out, std::string node, std::chrono::milliseconds flushInterval)
    : node_(std::move(node)),
      flushInterval_(flushInterval),
      out_(std::move(out)),
      flusher_([this] { flushLoop(); }) {}

Metrics::~Metrics() {
    {
        std::lock_guard lock(stopMu_);
        stopping_ = true;
    }
    stopCv_.notify_all();
    flusher_.join();
    flush();
}

Counter& Metrics::counter(const std::string& name, const std::string& labels) {
    std::lock_guard lock(mu_);
    return findOrCreate(counters_, name, labels);
}

Gauge& Metrics::gauge(const std::string& name, const std::string& labels) {
    std::lock_guard lock(mu_);
    return findOrCreate(gauges_, name, labels);
}

Histogram& Metrics::histogram(const std::string& name, const std::string& labels) {
    std::lock_guard lock(mu_);
    return findOrCreate(histograms_, name, labels);
}

void Metrics::flush() {
    const TimeMs now = time::wallMs();
    std::lock_guard lock(mu_);
    const auto row = [&](const std::string& name, const std::string& labels, auto value) {
        out_ << std::format("{},{},{},{},{}\n", now, node_, name, labels, value);
    };
    findOrCreate(gauges_, "process_rss_bytes", "").set(residentBytes());
    for (const auto& [key, c] : counters_) {
        row(key.first, key.second, c->value());
    }
    for (const auto& [key, g] : gauges_) {
        row(key.first, key.second, g->value());
    }
    for (const auto& [key, h] : histograms_) {
        auto samples = h->takeSamples();
        if (samples.empty()) {
            continue;  // no rows for an interval without samples
        }
        std::sort(samples.begin(), samples.end());
        row(key.first, withLabel(key.second, "quantile=p50"), quantile(samples, 0.50));
        row(key.first, withLabel(key.second, "quantile=p95"), quantile(samples, 0.95));
        row(key.first, withLabel(key.second, "quantile=p99"), quantile(samples, 0.99));
    }
    out_.flush();
}

void Metrics::flushLoop() {
    std::unique_lock lock(stopMu_);
    while (!stopCv_.wait_for(lock, flushInterval_, [this] { return stopping_; })) {
        lock.unlock();
        flush();
        lock.lock();
    }
}

}  // namespace elog
