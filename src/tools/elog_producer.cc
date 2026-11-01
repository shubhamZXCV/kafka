// elog-producer: a keyed synthetic workload (LLD §15.2, R-18). Every acked record is
// recorded in produced-<id>.csv as contiguous offset runs per partition (LLD §13.1).
//
//     elog-producer --coordinator HOST:PORT --topic T [--count N] [--rate R] [--keys K]
//                   [--payload B] [--seed S] [--id p1] [--out DIR]

#include <chrono>
#include <cstdio>
#include <fstream>
#include <future>
#include <map>
#include <random>
#include <string>
#include <thread>

#include "elog/client/producer.h"
#include "elog/common/bounded_queue.h"
#include "elog/common/logging.h"
#include "elog/common/metrics.h"
#include "elog/common/time.h"
#include "elog/protocol/app_value.h"
#include "elog/tools/cli.h"
#include "elog/tools/stop_signal.h"

namespace {

using namespace elog;

constexpr auto kFlushInterval = std::chrono::seconds(1);

// Resolves acks in send order and writes them as runs: one row per contiguous range of
// offsets acked on a partition since the last flush.
class AckWriter {
public:
    AckWriter(const std::string& path, std::string producerId, std::string topic)
        : out_(path), producerId_(std::move(producerId)), topic_(std::move(topic)) {
        out_ << "ts_ms,producer,topic,partition,base_offset,count\n";
    }

    bool ok() const { return out_.good(); }
    std::uint64_t failed() const { return failed_; }

    // Waits for one ack (they arrive in send order per partition).
    void take(std::future<Result<RecordMetadata>> ack) {
        auto result = ack.get();
        if (!result.ok()) {
            ++failed_;
            return;
        }
        Run& run = runs_[result.value().tp.partition];
        if (run.count > 0 && result.value().offset != run.base + run.count) {
            write(result.value().tp.partition, run);
            run.count = 0;
        }
        if (run.count == 0) {
            run.base = result.value().offset;
        }
        ++run.count;
        if (std::chrono::steady_clock::now() - lastFlush_ >= kFlushInterval) {
            flush();
        }
    }

    void flush() {
        for (auto& [partition, run] : runs_) {
            if (run.count > 0) {
                write(partition, run);
                run.count = 0;
            }
        }
        out_.flush();
        lastFlush_ = std::chrono::steady_clock::now();
    }

private:
    struct Run {
        Offset base = 0;
        std::uint64_t count = 0;
    };

    void write(std::uint32_t partition, const Run& run) {
        out_ << time::wallMs() << ',' << producerId_ << ',' << topic_ << ',' << partition << ','
             << run.base << ',' << run.count << '\n';
    }

    std::ofstream out_;
    const std::string producerId_;
    const std::string topic_;
    std::map<std::uint32_t, Run> runs_;
    std::uint64_t failed_ = 0;
    std::chrono::steady_clock::time_point lastFlush_ = std::chrono::steady_clock::now();
};

int run(const tools::Args& args, const tools::StopSignal& stop) {
    const auto count = args.getInt("--count"), rate = args.getInt("--rate"),
               keys = args.getInt("--keys"), payload = args.getInt("--payload"),
               seed = args.getInt("--seed");
    for (const auto* value : {&count, &rate, &keys, &payload, &seed}) {
        if (!value->ok() || value->value() < 0) {
            std::fprintf(stderr, "elog-producer: %s\n",
                         value->ok() ? "negative value" : value->message().c_str());
            return 2;
        }
    }
    const std::string id = args.get("--id"), topic = args.get("--topic"), out = args.get("--out");
    logging::init("producer-" + id, std::nullopt);
    auto metrics = Metrics::open(out, id, std::chrono::milliseconds(1000));
    AckWriter acks(out + "/produced-" + id + ".csv", id, topic);
    if (!metrics.ok() || !acks.ok()) {
        std::fprintf(stderr, "elog-producer: cannot write to %s\n", out.c_str());
        return 2;
    }

    Producer producer(ProducerConfig{args.get("--coordinator")}, *metrics.value());
    BoundedQueue<std::future<Result<RecordMetadata>>> pending(100'000);
    std::thread ackThread([&] {
        while (auto ack = pending.pop(std::chrono::milliseconds(100))) {
            acks.take(std::move(*ack));
        }
    });

    std::mt19937_64 random(static_cast<std::uint64_t>(seed.value()));
    const auto start = std::chrono::steady_clock::now();
    std::uint64_t sent = 0;
    for (; !stop.stopped() && (count.value() == 0 || sent < std::uint64_t(count.value())); ++sent) {
        if (rate.value() > 0) {  // pace to `rate` records per second
            std::this_thread::sleep_until(
                start + std::chrono::microseconds(sent * 1'000'000 / rate.value()));
        }
        const std::string key =
            "key-" + std::to_string(keys.value() > 0 ? random() % keys.value() : 0);
        const auto value = encodeAppValue({id, sent, time::wallMs(), std::size_t(payload.value())});
        auto ack = keys.value() > 0 ? producer.send(topic, std::as_bytes(std::span(key)), value)
                                    : producer.send(topic, std::nullopt, value);
        pending.push(std::move(ack), std::chrono::hours(1));
    }
    producer.flush();
    pending.close();
    ackThread.join();
    acks.flush();
    logging::get("producer")->info("finished sent={} failed={}", sent, acks.failed());
    return acks.failed() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    tools::StopSignal stop;  // first: every later thread inherits the blocked signals
    auto args = tools::parseArgs(argc, argv, 1,
                                 {{"--coordinator", "", true},
                                  {"--topic", "", true},
                                  {"--id", "p1"},
                                  {"--count", "0"},
                                  {"--rate", "0"},
                                  {"--keys", "100"},
                                  {"--payload", "100"},
                                  {"--seed", "1"},
                                  {"--out", "."}});
    if (!args.ok() || !args.value().positional().empty()) {
        std::fprintf(stderr, "elog-producer: %s\n",
                     args.ok() ? "unexpected argument" : args.message().c_str());
        return 2;
    }
    return run(args.value(), stop);
}
