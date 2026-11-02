#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "elog/client/assignment_state.h"
#include "elog/client/metadata_cache.h"
#include "elog/common/bounded_queue.h"
#include "elog/common/metrics.h"
#include "elog/common/result.h"
#include "elog/common/types.h"

namespace elog {

struct ConsumerConfig {
    std::string coordinator, groupId, clientId;
    std::vector<std::string> topics;
    std::vector<TopicPartition> assignment;  // manual mode (R-16): used when groupId is empty
    std::size_t inflightMax = 1'000;         // Q, the credit ceiling
    std::size_t fetchMaxBytes = 1 << 20;
    std::chrono::milliseconds fetchMaxWait{500};
    enum class Reset { Earliest, Latest } autoOffsetReset = Reset::Earliest;
};

struct ConsumedRecord {
    TopicPartition tp;
    Offset offset = 0;
    TimeMs timestampMs = 0;
    std::vector<std::byte> key, value;
    std::uint64_t assignedVersion = 0;  // internal: the assignment it was fetched under
};

class Fetcher;

// Reads records and hands them to `process` in offset order per partition (LLD §10.1,
// §10.4). The fetcher thread fills a bounded in-flight queue, asking brokers for no more
// records than the queue has free slots, so a slow `process` slows fetching instead of
// growing memory (FR-BP-01, FR-BP-02). Only manual mode (R-16) exists for now.
//
// Thread rules: run() is the processing thread; close() may be called from any thread.
class Consumer {
public:
    Consumer(ConsumerConfig cfg, Metrics& metrics);
    ~Consumer();

    // Runs until close(). `process` is called on this thread.
    Status run(const std::function<void(const ConsumedRecord&)>& process);
    void close();

private:
    Result<std::vector<TopicPartition>> partitionsToRead();

    const ConsumerConfig cfg_;
    Metrics& metrics_;
    MetadataCache metadata_;
    AssignmentState state_;
    BoundedQueue<ConsumedRecord> inflight_;
    std::unique_ptr<Fetcher> fetcher_;
    std::atomic<bool> closed_{false};
};

}  // namespace elog
