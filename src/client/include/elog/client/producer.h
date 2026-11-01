#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <map>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "elog/client/metadata_cache.h"
#include "elog/client/rpc_connection.h"
#include "elog/common/metrics.h"
#include "elog/common/result.h"
#include "elog/common/types.h"
#include "elog/protocol/record_batch.h"

namespace elog {

struct ProducerConfig {
    std::string coordinator;  // "host:port"
    std::size_t bufferMemoryBytes = 32 << 20;
    std::size_t batchMaxBytes = 64 << 10;
    std::uint32_t batchMaxRecords = 500;
    std::chrono::milliseconds linger{5};
    std::chrono::milliseconds requestTimeout{30'000};
    int retries = 10;
    std::chrono::milliseconds retryBackoff{100};
};

struct RecordMetadata {
    TopicPartition tp;
    Offset offset = 0;
};

// Batches records per partition and sends them from one sender thread (LLD §10.3). At most
// one batch per partition is in flight, and a failed batch is retried before any later
// one, so each partition's records are stored in send() order (INV-01).
//
// Thread rules: send(), flush() and close() may be called from any thread. mu_ guards the
// accumulator; network I/O happens on the sender thread without it.
class Producer {
public:
    Producer(ProducerConfig cfg, Metrics& metrics);
    ~Producer();  // close()

    // Blocks while the accumulator is full (FR-PROD-07). The future resolves once the
    // record is committed, or with the error that ended its retries.
    std::future<Result<RecordMetadata>> send(std::string_view topic,
                                             std::optional<std::span<const std::byte>> key,
                                             std::span<const std::byte> value);
    void flush();  // waits until every record sent so far is resolved
    void close();  // flush, then stop the sender

private:
    struct Batch {
        RecordBatchBuilder builder;
        std::vector<std::promise<Result<RecordMetadata>>> promises;
        std::vector<std::byte> bytes;  // built on first send, reused for retries
        std::size_t sizeBytes = 0;
        std::chrono::steady_clock::time_point createdAt;
        std::chrono::steady_clock::time_point retryAt;
        int attempts = 0;
    };
    struct PartitionQueue {
        std::deque<Batch> batches;
        bool inFlight = false;  // the head batch is being sent
    };

    Result<std::uint32_t> choosePartition(std::string_view topic,
                                          std::optional<std::span<const std::byte>> key);
    void senderLoop();
    std::optional<TopicPartition> nextReady(std::chrono::steady_clock::time_point now);
    ErrorCode sendBatch(const TopicPartition& tp, Batch& batch, Offset& baseOffset,
                        std::uint32_t& throttleMs);
    void finishBatch(const TopicPartition& tp, ErrorCode error, Offset baseOffset,
                     std::uint32_t throttleMs);

    const ProducerConfig cfg_;
    Metrics& metrics_;
    MetadataCache metadata_;
    std::map<std::pair<std::string, std::uint16_t>, RpcConnection> connections_;  // sender only

    std::mutex mu_;
    std::condition_variable changed_;  // accumulator changed: new batch, ack, space freed
    std::map<TopicPartition, PartitionQueue> queues_;
    std::size_t bufferedBytes_ = 0;
    std::size_t unresolvedRecords_ = 0;
    int flushers_ = 0;  // threads in flush(): every batch is ready regardless of linger
    bool closing_ = false;
    std::uint32_t roundRobin_ = 0;  // for records without a key (FR-PART-05)
    TopicPartition lastChecked_;    // where nextReady() resumes its search
    std::thread sender_;            // last: starts after everything above
};

}  // namespace elog
