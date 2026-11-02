#include "elog/broker/replica_fetcher.h"

#include <utility>

#include "elog/common/logging.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/msgpack_codec.h"

namespace elog {

namespace {

constexpr auto kErrorBackoff = std::chrono::milliseconds(100);
constexpr auto kConnectTimeout = std::chrono::milliseconds(1000);

}  // namespace

ReplicaFetcherThread::ReplicaFetcherThread(BrokerAddress source, ReplicaManager& replicas,
                                           const BrokerConfig& cfg)
    : source_(std::move(source)), replicas_(replicas), cfg_(cfg) {
    thread_ = std::thread([this] { run(); });
}

ReplicaFetcherThread::~ReplicaFetcherThread() {
    stop();
}

void ReplicaFetcherThread::setPartitions(std::vector<TopicPartition> partitions) {
    std::lock_guard lock(mu_);
    partitions_ = std::move(partitions);
}

void ReplicaFetcherThread::stop() {
    {
        std::lock_guard lock(mu_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

void ReplicaFetcherThread::run() {
    logging::get("replication")->info("fetcher_started leader={}", source_.id);
    while (sleepFor(std::chrono::milliseconds(0))) {
        fetchOnce();
    }
    logging::get("replication")->info("fetcher_stopped leader={}", source_.id);
}

// LLD §6.8: one request for every followed partition that is ready to fetch.
void ReplicaFetcherThread::fetchOnce() {
    std::vector<TopicPartition> partitions;
    {
        std::lock_guard lock(mu_);
        partitions = partitions_;
    }
    ReplicaFetchRequest request{cfg_.brokerId,
                                static_cast<std::uint32_t>(cfg_.replicaFetchWait.count()),
                                static_cast<std::uint32_t>(cfg_.replicaFetchMaxBytes),
                                {}};
    std::map<TopicPartition, std::pair<std::shared_ptr<Replica>, Epoch>> sent;
    for (const TopicPartition& tp : partitions) {
        const auto replica = replicas_.get(tp);
        const auto view = replica ? replica->followerView() : std::nullopt;
        if (!view || view->leader != source_.id) {
            continue;
        }
        if (view->state == FollowerState::TRUNCATING) {
            // Leader-epoch truncation comes next; a new follower's log is a prefix already.
            replica->setFollowerState(view->leaderEpoch, FollowerState::FETCHING);
        }
        request.partitions.push_back({tp.topic, tp.partition, view->leo, view->leaderEpoch});
        sent[tp] = {replica, view->leaderEpoch};
    }
    if (request.partitions.empty()) {
        sleepFor(kErrorBackoff);
        return;
    }
    auto frame = call(encode(request, 0));
    auto response = frame.ok() ? decodeReplicaFetchResponse(frame.value())
                               : Result<ReplicaFetchResponse>(frame.error(), frame.message());
    if (!response.ok()) {
        logging::get("replication")
            ->warn("replica_fetch_failed leader={} error=\"{}\"", source_.id, response.message());
        sleepFor(kErrorBackoff);
        return;
    }
    for (ReplicaFetchPartitionResponse& p : response.value().partitions) {
        const auto it = sent.find({p.topic, p.partition});
        if (it == sent.end()) {
            continue;
        }
        auto& [replica, epoch] = it->second;
        if (p.error == ErrorCode::OFFSET_OUT_OF_RANGE) {
            replica->setFollowerState(epoch, FollowerState::TRUNCATING);
        } else if (p.error == ErrorCode::NONE) {
            if (auto status = replica->appendAsFollower(p.records, p.highWatermark, epoch);
                !status.ok()) {
                logging::get("replication")
                    ->error("follower_append_failed topic={} partition={} error=\"{}\"", p.topic,
                            p.partition, status.message());
            }
        }  // FENCED_LEADER_EPOCH and others: wait for the next LeaderAndIsr
    }
}

// One request on the cached connection to the leader; reconnects after a failure.
Result<Frame> ReplicaFetcherThread::call(const Frame& request) {
    if (!socket_) {
        auto socket = Socket::connect(source_.host, source_.port, kConnectTimeout);
        if (!socket.ok()) {
            return {socket.error(), socket.message()};
        }
        const auto timeout = cfg_.replicaFetchWait + cfg_.requestTimeout;
        if (auto status = socket.value().setTimeouts(timeout, timeout); !status.ok()) {
            return {status.error(), status.message()};
        }
        socket_.emplace(std::move(socket).value());
    }
    Frame stamped = request;
    stamped.correlationId = nextCorrelationId_++;
    auto response = writeFrame(*socket_, stamped).ok()
                        ? readFrame(*socket_, cfg_.maxFrameBytes)
                        : Result<Frame>(ErrorCode::NETWORK_ERROR, "send to leader failed");
    if (!response.ok() || response.value().correlationId != stamped.correlationId) {
        socket_.reset();
        return response.ok() ? Result<Frame>(ErrorCode::CORRUPT_MESSAGE, "correlation mismatch")
                             : response;
    }
    return response;
}

bool ReplicaFetcherThread::sleepFor(std::chrono::milliseconds duration) {
    std::unique_lock lock(mu_);
    return !cv_.wait_for(lock, duration, [&] { return stopping_; });
}

ReplicaFetcherManager::ReplicaFetcherManager(ReplicaManager& replicas, const BrokerConfig& cfg)
    : replicas_(replicas), cfg_(cfg) {}

// Groups the followed partitions by leader; a thread whose leader moved address, or who
// has nothing left to follow, is stopped (LLD §6.8).
void ReplicaFetcherManager::reconcile(const std::vector<BrokerAddress>& brokers) {
    std::lock_guard lock(mu_);
    for (const BrokerAddress& b : brokers) {
        addresses_[b.id] = b;
    }
    std::map<BrokerId, std::vector<TopicPartition>> byLeader;
    for (const auto& replica : replicas_.all()) {
        if (const auto view = replica->followerView()) {
            byLeader[view->leader].push_back(replica->topicPartition());
        }
    }
    for (auto it = threads_.begin(); it != threads_.end();) {
        const auto address = addresses_.find(it->first);
        const bool moved = address == addresses_.end() ||
                           address->second.host != it->second->source().host ||
                           address->second.port != it->second->source().port;
        if (!byLeader.contains(it->first) || moved) {
            it->second->stop();
            it = threads_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto& [leader, partitions] : byLeader) {
        const auto address = addresses_.find(leader);
        if (address == addresses_.end()) {
            continue;  // leader address unknown until the next LeaderAndIsr
        }
        auto& thread = threads_[leader];
        if (!thread) {
            thread = std::make_unique<ReplicaFetcherThread>(address->second, replicas_, cfg_);
        }
        thread->setPartitions(std::move(partitions));
    }
}

void ReplicaFetcherManager::stopAll() {
    std::lock_guard lock(mu_);
    threads_.clear();  // each destructor stops and joins its thread
}

}  // namespace elog
