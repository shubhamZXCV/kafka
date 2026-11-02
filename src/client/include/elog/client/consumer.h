#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "elog/client/assignment_state.h"
#include "elog/client/metadata_cache.h"
#include "elog/common/bounded_queue.h"
#include "elog/common/metrics.h"
#include "elog/common/result.h"
#include "elog/common/types.h"

namespace elog {

// LLD §14, R-24: crash once k records were processed since the last successful commit.
struct CrashAfterProcess {
    std::uint32_t k = 0;
};

struct ConsumerConfig {
    std::string coordinator, groupId, clientId;
    std::vector<std::string> topics;
    std::vector<TopicPartition> assignment;  // manual mode (R-16): used when groupId is empty
    std::size_t inflightMax = 1'000;         // Q, the credit ceiling
    std::size_t fetchMaxBytes = 1 << 20;
    std::chrono::milliseconds fetchMaxWait{500};
    std::chrono::milliseconds sessionTimeout{6'000};  // group mode (LLD §10.1)
    std::chrono::milliseconds heartbeatInterval{2'000};
    std::chrono::milliseconds rebalanceTimeout{10'000};
    std::uint32_t commitIntervalRecords = 100;            // N (E8)
    std::chrono::milliseconds commitIntervalTime{1'000};  // negative: disabled (E8)
    // R-21: runs on the processing thread before every OffsetCommit, e.g. to persist a
    // record of what was processed so it is never behind the committed offsets.
    std::function<void()> beforeCommit;
    std::optional<CrashAfterProcess> crashAfterProcess;  // group mode only (§14)
    std::function<void()> beforeCrash;                   // R-24: runs just before the SIGKILL
    enum class Reset { Earliest, Latest } autoOffsetReset = Reset::Earliest;
};

struct ConsumedRecord {
    TopicPartition tp;
    Offset offset = 0;
    TimeMs timestampMs = 0;
    std::vector<std::byte> key, value;
    Epoch epoch = 0;                    // the partition's ownership epoch (0 in manual mode)
    std::uint64_t assignedVersion = 0;  // internal: the assignment it was fetched under
};

class Fetcher;
class MembershipClient;
class OffsetCommitter;

// Reads records and hands them to `process` in offset order per partition (LLD §10.1,
// §10.4). The fetcher thread fills a bounded in-flight queue, asking brokers for no more
// records than the queue has free slots, so a slow `process` slows fetching instead of
// growing memory (FR-BP-01, FR-BP-02). With a groupId the consumer joins that group and
// commits its progress (LLD §10.6); without one it reads a fixed assignment (R-16).
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
    Status startManual();
    Status startGroup();
    Status joinAndApply();
    std::map<TopicPartition, Offset> committedOffsets(const std::vector<TopicPartition>& tps);
    void onRebalance();
    void onFenced();
    void doCommit();
    void maybeCrash();
    void dropBuffered(const std::vector<TopicPartition>& partitions);
    void dropAllBuffered();

    const ConsumerConfig cfg_;
    Metrics& metrics_;
    MetadataCache metadata_;
    AssignmentState state_;
    BoundedQueue<ConsumedRecord> inflight_;
    std::unique_ptr<Fetcher> fetcher_;
    std::unique_ptr<MembershipClient> membership_;  // group mode only
    std::unique_ptr<OffsetCommitter> committer_;    // group mode only
    std::atomic<bool> closed_{false};
    bool eagerAssignor_ = true;  // from the last JoinGroup; processing thread only
    // R-24: records processed since the last fully accepted commit; none before the first.
    std::optional<std::uint32_t> processedSinceCommit_;
};

}  // namespace elog
