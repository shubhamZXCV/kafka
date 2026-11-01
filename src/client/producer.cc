#include "elog/client/producer.h"

#include <algorithm>
#include <utility>

#include "elog/client/partitioner.h"
#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/msgpack_codec.h"

namespace elog {

namespace {

using Clock = std::chrono::steady_clock;

constexpr std::size_t kRecordOverheadBytes = 32;            // rough per-record encoding cost
constexpr auto kSenderPoll = std::chrono::milliseconds(5);  // linger and backoff resolution

// LLD §10.3: errors a later attempt can get past (new leader, freed capacity, a broker
// that has not received its LeaderAndIsr yet, a dropped connection).
bool retriable(ErrorCode error) {
    switch (error) {
        case ErrorCode::NETWORK_ERROR:
        case ErrorCode::NOT_LEADER:
        case ErrorCode::FENCED_LEADER_EPOCH:
        case ErrorCode::UNKNOWN_TOPIC_OR_PARTITION:
        case ErrorCode::NOT_ENOUGH_REPLICAS:
        case ErrorCode::THROTTLED:
        case ErrorCode::REQUEST_TIMED_OUT:
            return true;
        default:
            return false;
    }
}

}  // namespace

Producer::Producer(ProducerConfig cfg, Metrics& metrics)
    : cfg_(std::move(cfg)), metrics_(metrics), metadata_(cfg_.coordinator) {
    sender_ = std::thread([this] { senderLoop(); });
}

Producer::~Producer() {
    close();
}

std::future<Result<RecordMetadata>> Producer::send(std::string_view topic,
                                                   std::optional<std::span<const std::byte>> key,
                                                   std::span<const std::byte> value) {
    std::promise<Result<RecordMetadata>> promise;
    auto future = promise.get_future();
    auto partition = choosePartition(topic, key);
    if (!partition.ok()) {
        promise.set_value(Result<RecordMetadata>(partition.error(), partition.message()));
        return future;
    }
    const std::size_t recordBytes = value.size() + (key ? key->size() : 0) + kRecordOverheadBytes;

    std::unique_lock lock(mu_);
    // FR-PROD-07: block the caller while the buffer is full (one record always fits).
    changed_.wait(lock, [&] {
        return closing_ || bufferedBytes_ == 0 ||
               bufferedBytes_ + recordBytes <= cfg_.bufferMemoryBytes;
    });
    if (closing_) {
        promise.set_value(Result<RecordMetadata>(ErrorCode::INVALID_REQUEST, "producer closed"));
        return future;
    }
    PartitionQueue& queue = queues_[{std::string(topic), partition.value()}];
    const auto full = [&](const Batch& b) {
        return !b.bytes.empty() || b.builder.count() >= cfg_.batchMaxRecords ||
               b.sizeBytes + recordBytes > cfg_.batchMaxBytes;
    };
    if (queue.batches.empty() || full(queue.batches.back())) {
        queue.batches.emplace_back();
        queue.batches.back().createdAt = Clock::now();
    }
    Batch& batch = queue.batches.back();
    batch.builder.add(key, value, time::wallMs());
    batch.promises.push_back(std::move(promise));
    batch.sizeBytes += recordBytes;
    bufferedBytes_ += recordBytes;
    ++unresolvedRecords_;
    changed_.notify_all();
    return future;
}

Result<std::uint32_t> Producer::choosePartition(std::string_view topic,
                                                std::optional<std::span<const std::byte>> key) {
    const std::string name(topic);
    auto count = metadata_.partitionCount(name);
    if (!count) {
        if (auto status = metadata_.refresh({name}); !status.ok()) {
            return {status.error(), status.message()};
        }
        count = metadata_.partitionCount(name);
    }
    if (!count || *count == 0) {
        return {ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, "no partitions for " + name};
    }
    if (key) {
        return partitionFor(*key, *count);  // FR-PART-04
    }
    std::lock_guard lock(mu_);
    return roundRobin_++ % *count;  // FR-PART-05
}

void Producer::flush() {
    std::unique_lock lock(mu_);
    ++flushers_;
    changed_.notify_all();
    changed_.wait(lock, [&] { return unresolvedRecords_ == 0; });
    --flushers_;
}

void Producer::close() {
    if (!sender_.joinable()) {
        return;
    }
    flush();
    {
        std::lock_guard lock(mu_);
        closing_ = true;
    }
    changed_.notify_all();
    sender_.join();
}

// The sender takes one ready head batch at a time, sends it without holding mu_, and then
// resolves or re-queues it. The batch stays at the head of its partition queue until it
// is resolved, which is what keeps per-partition order across retries (INV-01).
void Producer::senderLoop() {
    std::unique_lock lock(mu_);
    while (true) {
        const auto tp = nextReady(Clock::now());
        if (!tp) {
            if (closing_ && unresolvedRecords_ == 0) {
                return;
            }
            changed_.wait_for(lock, kSenderPoll);
            continue;
        }
        PartitionQueue& queue = queues_[*tp];
        queue.inFlight = true;
        Batch& batch = queue.batches.front();  // stays valid: only this thread pops
        if (batch.bytes.empty()) {
            batch.bytes = batch.builder.build();
        }
        lock.unlock();
        Offset baseOffset = 0;
        std::uint32_t throttleMs = 0;
        const ErrorCode error = sendBatch(*tp, batch, baseOffset, throttleMs);
        lock.lock();
        finishBatch(*tp, error, baseOffset, throttleMs);
    }
}

// Requires mu_. Searches round-robin from the partition after the last one checked, so a
// partition with a backlog cannot starve the others.
std::optional<TopicPartition> Producer::nextReady(Clock::time_point now) {
    const auto ready = [&](const PartitionQueue& q) {
        if (q.inFlight || q.batches.empty() || q.batches.front().retryAt > now) {
            return false;
        }
        const Batch& head = q.batches.front();
        return q.batches.size() > 1 || head.builder.count() >= cfg_.batchMaxRecords ||
               head.sizeBytes >= cfg_.batchMaxBytes || now - head.createdAt >= cfg_.linger ||
               flushers_ > 0 || closing_;
    };
    auto it = queues_.upper_bound(lastChecked_);
    for (std::size_t checked = 0; checked < queues_.size(); ++checked, ++it) {
        if (it == queues_.end()) {
            it = queues_.begin();
        }
        if (ready(it->second)) {
            lastChecked_ = it->first;
            return it->first;
        }
    }
    return std::nullopt;
}

// Sender thread, without mu_.
ErrorCode Producer::sendBatch(const TopicPartition& tp, Batch& batch, Offset& baseOffset,
                              std::uint32_t& throttleMs) {
    if (batch.attempts > 0 || !metadata_.leaderFor(tp)) {
        metadata_.refresh({tp.topic});  // a failure shows up as a missing leader below
    }
    const auto leader = metadata_.leaderFor(tp);
    if (!leader) {
        return ErrorCode::NOT_LEADER;
    }
    auto [it, added] =
        connections_.try_emplace({leader->broker.host, leader->broker.port}, leader->broker.host,
                                 leader->broker.port, cfg_.requestTimeout);
    ProduceRequest request{static_cast<std::uint32_t>(cfg_.requestTimeout.count()),
                           {{tp.topic, tp.partition, leader->leaderEpoch, batch.bytes}}};
    auto frame = it->second.call(encode(request, 0));
    if (!frame.ok()) {
        return frame.error();
    }
    auto response = decodeProduceResponse(frame.value());
    if (!response.ok() || response.value().partitions.size() != 1) {
        return ErrorCode::CORRUPT_MESSAGE;
    }
    const ProducePartitionResponse& p = response.value().partitions.front();
    baseOffset = p.baseOffset;
    throttleMs = p.throttleMs;
    return p.error;
}

// Requires mu_.
void Producer::finishBatch(const TopicPartition& tp, ErrorCode error, Offset baseOffset,
                           std::uint32_t throttleMs) {
    PartitionQueue& queue = queues_[tp];
    queue.inFlight = false;
    Batch& batch = queue.batches.front();
    if (error != ErrorCode::NONE && retriable(error) && ++batch.attempts <= cfg_.retries) {
        const auto backoff = std::max(cfg_.retryBackoff, std::chrono::milliseconds(throttleMs));
        batch.retryAt = Clock::now() + backoff;
        return;
    }
    for (std::size_t i = 0; i < batch.promises.size(); ++i) {
        if (error == ErrorCode::NONE) {
            batch.promises[i].set_value(RecordMetadata{tp, baseOffset + i});
        } else {
            batch.promises[i].set_value(Result<RecordMetadata>(
                error, "produce to " + tp.topic + "-" + std::to_string(tp.partition) + " failed"));
        }
    }
    if (error == ErrorCode::NONE) {
        const std::string labels =
            "topic=" + tp.topic + ";partition=" + std::to_string(tp.partition);
        metrics_.counter("produce_records_total", labels)
            .add(static_cast<std::int64_t>(batch.promises.size()));
        metrics_.histogram("produce_latency_ms")
            .record(
                std::chrono::duration<double, std::milli>(Clock::now() - batch.createdAt).count());
    } else {
        logging::get("producer")
            ->warn("batch_failed topic={} partition={} records={} attempts={} error={}", tp.topic,
                   tp.partition, batch.promises.size(), batch.attempts, errorName(error));
    }
    bufferedBytes_ -= batch.sizeBytes;
    unresolvedRecords_ -= batch.promises.size();
    queue.batches.pop_front();
    changed_.notify_all();
}

}  // namespace elog
