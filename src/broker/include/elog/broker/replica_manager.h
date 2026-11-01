#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "elog/broker/replica.h"
#include "elog/common/result.h"
#include "elog/common/types.h"

namespace elog {

// The partitions this broker hosts: TopicPartition → Replica (LLD §6.1). Each replica's log
// lives in <dataDir>/<topic>-<partition>.
//
// Thread rules: every method is safe from any thread. Lock order (LLD §11.2, R-11):
// createMu_ → mapMu_ → Replica::mu_. mapMu_ is never held across I/O; createMu_ is, and
// only getOrCreate takes it.
class ReplicaManager {
public:
    ReplicaManager(std::string dataDir, LogConfig logConfig, std::ptrdiff_t maxPendingProduce,
                   HwNotifier& hwNotifier);

    // The replica for tp, or nullptr if this broker does not host it.
    std::shared_ptr<Replica> get(const TopicPartition& tp) const;

    // The replica for tp, opening its log first if this broker does not host it yet
    // (LLD §6.3).
    Result<std::shared_ptr<Replica>> getOrCreate(const TopicPartition& tp);

    std::vector<std::shared_ptr<Replica>> all() const;

    // Applies a LeaderAndIsr from the coordinator to every partition it names, opening logs
    // for new ones (LLD §6.3). `self` is this broker's ID.
    LeaderAndIsrResponse applyLeaderAndIsr(const LeaderAndIsrRequest& request, BrokerId self);

    // Scheduler jobs (LLD §6.10): flush every log; write the recovery-point and
    // high-watermark checkpoints into dataDir. Each logs its own failures.
    void flushLogs();
    void checkpointRecoveryPoints();
    void checkpointHighWatermarks();
    void applyRetention();

private:
    const std::string dataDir_;
    const LogConfig logConfig_;
    std::map<TopicPartition, Offset> recoveryPoints_;  // from the checkpoint at startup
    const std::ptrdiff_t maxPendingProduce_;           // admission slots per new replica
    HwNotifier& hwNotifier_;
    std::mutex createMu_;
    mutable std::shared_mutex mapMu_;
    std::map<TopicPartition, std::shared_ptr<Replica>> replicas_;
};

}  // namespace elog
