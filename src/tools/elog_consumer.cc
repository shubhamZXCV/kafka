// elog-consumer: consumes a topic and records what it processed (LLD §15.2, R-18) as
// offset ranges per partition in processed-<id>.csv (LLD §13.1).
//
//     elog-consumer --coordinator HOST:PORT --topic T (--manual [--partitions 0,1,...] |
//                   --group G [--session-timeout-ms S] [--heartbeat-interval-ms H]
//                   [--rebalance-timeout-ms R] [--commit-interval-records N]
//                   [--commit-interval-ms M]) [--count N] [--inflight Q]
//                   [--reset earliest|latest] [--process-us U]
//                   [--crash-after-process K|random [--seed S]] [--id c1] [--out DIR]

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <thread>

#include "elog/client/consumer.h"
#include "elog/common/logging.h"
#include "elog/common/metrics.h"
#include "elog/common/time.h"
#include "elog/tools/cli.h"
#include "elog/tools/stop_signal.h"

namespace {

using namespace elog;

constexpr auto kFlushInterval = std::chrono::seconds(1);
constexpr std::uint64_t kFlushRecords = 1000;

// processed-<id>.csv: one row per contiguous run of processed offsets on a partition,
// written at least every second (ranges, not per record).
class RangeWriter {
public:
    RangeWriter(const std::string& path, std::string consumerId)
        : out_(tools::openCsv(
              path, "ts_ms,consumer,topic,partition,start_offset,end_offset_excl,owner_epoch")),
          consumerId_(std::move(consumerId)) {}

    bool ok() const { return out_.good(); }

    void processed(const ConsumedRecord& record) {
        Run& run = runs_[record.tp];
        if (run.end > run.start && (record.offset != run.end || record.epoch != run.epoch)) {
            write(record.tp, run);
            run = {};
        }
        if (run.end == run.start) {
            run = {record.offset, record.offset, record.epoch};
        }
        run.end = record.offset + 1;
        if (++sinceFlush_ >= kFlushRecords ||
            std::chrono::steady_clock::now() - lastFlush_ >= kFlushInterval) {
            flush();
        }
    }

    void flush() {
        for (auto& [tp, run] : runs_) {
            if (run.end > run.start) {
                write(tp, run);
                run = {run.end, run.end, run.epoch};
            }
        }
        out_.flush();
        sinceFlush_ = 0;
        lastFlush_ = std::chrono::steady_clock::now();
    }

private:
    struct Run {
        Offset start = 0, end = 0;  // [start, end)
        Epoch epoch = 0;            // ownership epoch; 0 in manual mode (R-16)
    };

    // A run never spans two ownership epochs: the order check is per epoch (LLD §13.4).
    void write(const TopicPartition& tp, const Run& run) {
        out_ << time::wallMs() << ',' << consumerId_ << ',' << tp.topic << ',' << tp.partition
             << ',' << run.start << ',' << run.end << ',' << run.epoch << '\n';
    }

    std::ofstream out_;
    const std::string consumerId_;
    std::map<TopicPartition, Run> runs_;
    std::uint64_t sinceFlush_ = 0;
    std::chrono::steady_clock::time_point lastFlush_ = std::chrono::steady_clock::now();
};

// LLD §14: --crash-after-process=K, or =random for K ~ Uniform[0, N) drawn from --seed,
// N being the commit interval.
Result<std::optional<CrashAfterProcess>> crashAfterProcess(const tools::Args& args,
                                                           std::uint32_t commitInterval) {
    const std::string& value = args.get("--crash-after-process");
    if (value.empty()) {
        return std::optional<CrashAfterProcess>{};
    }
    if (args.get("--group").empty()) {
        return {ErrorCode::INVALID_REQUEST, "--crash-after-process needs --group"};
    }
    if (value == "random") {
        const auto seed = args.getInt("--seed");
        if (!seed.ok()) {
            return {seed.error(), seed.message()};
        }
        std::mt19937_64 random(static_cast<std::uint64_t>(seed.value()));
        const auto k = static_cast<std::uint32_t>(random() % commitInterval);
        return std::optional<CrashAfterProcess>{CrashAfterProcess{k}};
    }
    if (value.find_first_not_of("0123456789") != std::string::npos || value.size() > 9) {
        return {ErrorCode::INVALID_REQUEST, "--crash-after-process must be a count or random"};
    }
    return std::optional<CrashAfterProcess>{
        CrashAfterProcess{static_cast<std::uint32_t>(std::stoul(value))}};
}

Result<ConsumerConfig> configFrom(const tools::Args& args) {
    if (args.has("--manual") == !args.get("--group").empty()) {
        return {ErrorCode::INVALID_REQUEST, "give exactly one of --manual and --group"};
    }
    ConsumerConfig cfg;
    cfg.groupId = args.get("--group");
    for (const auto& [flag, field] : {std::pair{"--session-timeout-ms", &cfg.sessionTimeout},
                                      std::pair{"--heartbeat-interval-ms", &cfg.heartbeatInterval},
                                      std::pair{"--rebalance-timeout-ms", &cfg.rebalanceTimeout},
                                      std::pair{"--commit-interval-ms", &cfg.commitIntervalTime}}) {
        const auto value = args.getInt(flag);
        if (!value.ok()) {
            return {value.error(), value.message()};
        }
        *field = std::chrono::milliseconds(value.value());  // negative commit interval: off
    }
    const auto commitRecords = args.getInt("--commit-interval-records");
    if (!commitRecords.ok() || commitRecords.value() < 1) {
        return {ErrorCode::INVALID_REQUEST, "--commit-interval-records must be at least 1"};
    }
    cfg.commitIntervalRecords = static_cast<std::uint32_t>(commitRecords.value());
    cfg.coordinator = args.get("--coordinator");
    cfg.clientId = args.get("--id");
    cfg.topics = {args.get("--topic")};
    std::stringstream partitions(args.get("--partitions"));
    for (std::string p; std::getline(partitions, p, ',');) {
        if (p.empty() || p.find_first_not_of("0123456789") != std::string::npos) {
            return {ErrorCode::INVALID_REQUEST, "bad partition '" + p + "'"};
        }
        cfg.assignment.push_back({args.get("--topic"), static_cast<std::uint32_t>(std::stoul(p))});
    }
    const auto inflight = args.getInt("--inflight");
    if (!inflight.ok() || inflight.value() < 1) {
        return {ErrorCode::INVALID_REQUEST, "--inflight must be at least 1"};
    }
    cfg.inflightMax = static_cast<std::size_t>(inflight.value());
    const std::string& reset = args.get("--reset");
    if (reset != "earliest" && reset != "latest") {
        return {ErrorCode::INVALID_REQUEST, "--reset must be earliest or latest"};
    }
    cfg.autoOffsetReset =
        reset == "latest" ? ConsumerConfig::Reset::Latest : ConsumerConfig::Reset::Earliest;
    auto crash = crashAfterProcess(args, cfg.commitIntervalRecords);
    if (!crash.ok()) {
        return {crash.error(), crash.message()};
    }
    cfg.crashAfterProcess = crash.value();
    return cfg;
}

int run(const tools::Args& args, const tools::StopSignal& stop) {
    auto cfg = configFrom(args);
    const auto count = args.getInt("--count");
    const auto processUs = args.getInt("--process-us");  // R-22: the E7 "callback sleep"
    if (!cfg.ok() || !count.ok() || count.value() < 0 || !processUs.ok() || processUs.value() < 0) {
        std::fprintf(stderr, "elog-consumer: %s\n",
                     cfg.ok() ? "bad --count or --process-us" : cfg.message().c_str());
        return 2;
    }
    const std::string id = args.get("--id"), out = args.get("--out");
    logging::init("consumer-" + id, std::nullopt);
    auto metrics = Metrics::open(out, id, std::chrono::milliseconds(1000));
    RangeWriter ranges(out + "/processed-" + id + ".csv", id);
    if (!metrics.ok() || !ranges.ok()) {
        std::fprintf(stderr, "elog-consumer: cannot write to %s\n", out.c_str());
        return 2;
    }
    cfg.value().beforeCommit = [&ranges] { ranges.flush(); };  // R-21
    cfg.value().beforeCrash = [&ranges] { ranges.flush(); };   // R-24
    Consumer consumer(std::move(cfg).value(), *metrics.value());
    std::atomic<bool> done{false};
    std::thread watcher([&] {  // SIGINT/SIGTERM → close(), which ends run()
        while (!done && !stop.stopped()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        consumer.close();
    });
    std::uint64_t processed = 0;
    const auto status = consumer.run([&](const ConsumedRecord& record) {
        if (processUs.value() > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(processUs.value()));
        }
        ranges.processed(record);
        if (++processed == std::uint64_t(count.value())) {
            consumer.close();
        }
    });
    done = true;
    watcher.join();
    ranges.flush();
    logging::get("consumer")->info("finished processed={}", processed);
    if (!status.ok()) {
        std::fprintf(stderr, "elog-consumer: %s\n", status.message().c_str());
        return 1;
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    tools::StopSignal stop;  // first: every later thread inherits the blocked signals
    auto args = tools::parseArgs(argc, argv, 1,
                                 {{"--coordinator", "", true},
                                  {"--topic", "", true},
                                  {"--manual", "", false, true},
                                  {"--group", ""},
                                  {"--session-timeout-ms", "6000"},
                                  {"--heartbeat-interval-ms", "2000"},
                                  {"--rebalance-timeout-ms", "10000"},
                                  {"--commit-interval-records", "100"},
                                  {"--commit-interval-ms", "1000"},
                                  {"--partitions", ""},
                                  {"--id", "c1"},
                                  {"--count", "0"},
                                  {"--inflight", "1000"},
                                  {"--reset", "earliest"},
                                  {"--process-us", "0"},
                                  {"--crash-after-process", ""},
                                  {"--seed", "1"},
                                  {"--out", "."}});
    if (!args.ok() || !args.value().positional().empty()) {
        std::fprintf(stderr, "elog-consumer: %s\n",
                     args.ok() ? "unexpected argument" : args.message().c_str());
        return 2;
    }
    return run(args.value(), stop);
}
