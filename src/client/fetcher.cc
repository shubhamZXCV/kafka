#include "elog/client/fetcher.h"

#include <algorithm>

#include "elog/common/logging.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/msgpack_codec.h"
#include "elog/protocol/record_batch.h"

namespace elog {

namespace {

constexpr auto kErrorPause = std::chrono::milliseconds(100);
constexpr auto kPushTimeout = std::chrono::hours(1);  // never reached: credit is honoured

std::string labelsFor(const TopicPartition& tp) {
    return "topic=" + tp.topic + ";partition=" + std::to_string(tp.partition);
}

}  // namespace

Fetcher::Fetcher(const ConsumerConfig& cfg, MetadataCache& metadata,
                 BoundedQueue<ConsumedRecord>& inflight, Metrics& metrics)
    : cfg_(cfg),
      metadata_(metadata),
      inflight_(inflight),
      metrics_(metrics),
      lowWater_(static_cast<std::uint32_t>(std::max<std::size_t>(1, cfg.inflightMax / 2))) {}

void Fetcher::start(std::vector<TopicPartition> partitions) {
    partitions_ = std::move(partitions);
    thread_ = std::thread([this] { loop(); });
}

void Fetcher::stop() {
    stopping_ = true;
    inflight_.close();  // wakes a wait for free slots
    if (thread_.joinable()) {
        thread_.join();
    }
}

void Fetcher::loop() {
    // R-16: "latest" means the HW when the consumer starts, so resolve every starting
    // position before the first fetch; any that fail are retried on later rounds.
    for (const TopicPartition& tp : partitions_) {
        startingOffset(tp);
    }
    while (!stopping_) {
        const auto credit = static_cast<std::uint32_t>(inflight_.freeSlots());
        metrics_.gauge("fetch_credit").set(credit);
        metrics_.gauge("inflight_depth").set(static_cast<std::int64_t>(inflight_.size()));
        // FR-FETCH-05: never fetch without credit. R-17: and wait for half the queue to be
        // free unless it is empty, so fetches are not one record each.
        if (credit == 0 || (credit < lowWater_ && inflight_.size() > 0)) {
            inflight_.waitFreeSlots(lowWater_, std::chrono::milliseconds(100));
            continue;
        }
        // Group by leader, starting at a different partition each round (fairness).
        std::map<BrokerId, std::pair<BrokerAddress, std::vector<TopicPartition>>> byLeader;
        for (std::size_t i = 0; i < partitions_.size(); ++i) {
            const TopicPartition& tp = partitions_[(i + round_) % partitions_.size()];
            const auto leader = metadata_.leaderFor(tp);
            if (!leader) {
                metadata_.refresh({tp.topic});
                continue;
            }
            auto& group = byLeader[leader->broker.id];
            group.first = leader->broker;
            group.second.push_back(tp);
        }
        ++round_;
        if (byLeader.empty()) {
            pause();
            continue;
        }
        // R-17: long-poll only when the last round found nothing anywhere; otherwise an
        // idle leader's long-poll would stall the busy ones behind it.
        const bool longPoll = recordsLastRound_ == 0;
        recordsLastRound_ = 0;
        for (const auto& [id, group] : byLeader) {
            const auto free = static_cast<std::uint32_t>(inflight_.freeSlots());
            if (free == 0 || stopping_) {
                break;
            }
            fetchFrom(group.first, group.second, free, longPoll);
        }
    }
}

void Fetcher::fetchFrom(const BrokerAddress& leader, const std::vector<TopicPartition>& partitions,
                        std::uint32_t credit, bool longPoll) {
    FetchRequest request{longPoll ? static_cast<std::uint32_t>(cfg_.fetchMaxWait.count()) : 0,
                         static_cast<std::uint32_t>(cfg_.fetchMaxBytes),
                         credit,
                         {}};
    for (const TopicPartition& tp : partitions) {
        const auto position = startingOffset(tp);
        const auto epoch = metadata_.leaderFor(tp);
        if (position && epoch) {
            request.partitions.push_back({tp.topic, tp.partition, *position, epoch->leaderEpoch});
        }
    }
    if (request.partitions.empty()) {
        return;
    }
    auto frame = connectionTo(leader).call(encode(request, 0));
    auto response = frame.ok() ? decodeFetchResponse(frame.value())
                               : Result<FetchResponse>(frame.error(), frame.message());
    if (!response.ok()) {
        logging::get("consumer")
            ->warn("fetch_failed broker={} error=\"{}\"", leader.id, response.message());
        metadata_.refresh({});
        pause();
        return;
    }
    for (const FetchPartitionResponse& p : response.value().partitions) {
        accept(p, {p.topic, p.partition});
    }
}

// Pushes the records of one partition and advances its position.
void Fetcher::accept(const FetchPartitionResponse& response, const TopicPartition& tp) {
    if (response.error == ErrorCode::OFFSET_OUT_OF_RANGE) {
        logging::get("consumer")
            ->warn("offset_out_of_range topic={} partition={} position={}", tp.topic, tp.partition,
                   positions_[tp]);
        // R-16, R-19: reset per auto.offset.reset, from the bounds the broker reported.
        positions_[tp] = cfg_.autoOffsetReset == ConsumerConfig::Reset::Earliest
                             ? response.logStartOffset
                             : response.highWatermark;
        return;
    }
    if (response.error != ErrorCode::NONE) {
        metadata_.refresh({tp.topic});  // NOT_LEADER, FENCED_LEADER_EPOCH, unknown partition
        return;
    }
    std::span<const std::byte> rest = response.records;
    while (rest.size() >= batch::kHeaderBytes) {
        const std::size_t size = batch::kRecordBatchOverhead + readBatchHeader(rest).batchLength;
        auto view = RecordBatchView::parse(rest.first(std::min(size, rest.size())));
        if (!view.ok()) {
            logging::get("consumer")
                ->error("corrupt_batch topic={} partition={} error=\"{}\"", tp.topic, tp.partition,
                        view.message());
            return;
        }
        view.value().forEachRecord([&](const Record& r) {
            if (r.offset < positions_[tp]) {
                return;
            }
            ConsumedRecord record{
                tp, r.offset, r.timestampMs, {}, {r.value.begin(), r.value.end()}};
            if (r.key) {
                record.key.assign(r.key->begin(), r.key->end());
            }
            positions_[tp] = r.offset + 1;
            ++recordsLastRound_;
            inflight_.push(std::move(record), kPushTimeout);
        });
        rest = rest.subspan(size);
    }
    metrics_.gauge("consumer_lag_records", labelsFor(tp))
        .set(static_cast<std::int64_t>(response.highWatermark) -
             static_cast<std::int64_t>(positions_[tp]));
}

// R-16: earliest is offset 0; latest is the leader's current HW, learnt with a fetch that
// asks for no records.
std::optional<Offset> Fetcher::startingOffset(const TopicPartition& tp) {
    if (const auto it = positions_.find(tp); it != positions_.end()) {
        return it->second;
    }
    if (cfg_.autoOffsetReset == ConsumerConfig::Reset::Earliest) {
        return positions_[tp] = 0;
    }
    const auto leader = metadata_.leaderFor(tp);
    if (!leader) {
        return std::nullopt;
    }
    auto frame =
        connectionTo(leader->broker)
            .call(encode(FetchRequest{0, 0, 0, {{tp.topic, tp.partition, 0, leader->leaderEpoch}}},
                         0));
    auto response = frame.ok() ? decodeFetchResponse(frame.value())
                               : Result<FetchResponse>(frame.error(), frame.message());
    if (!response.ok() || response.value().partitions.size() != 1 ||
        response.value().partitions[0].error != ErrorCode::NONE) {
        return std::nullopt;
    }
    return positions_[tp] = response.value().partitions[0].highWatermark;
}

RpcConnection& Fetcher::connectionTo(const BrokerAddress& broker) {
    auto [it, added] =
        connections_.try_emplace({broker.host, broker.port}, broker.host, broker.port);
    return it->second;
}

// Sleeps in short steps so stop() stays responsive.
void Fetcher::pause() {
    for (auto waited = std::chrono::milliseconds(0); waited < kErrorPause && !stopping_;
         waited += std::chrono::milliseconds(10)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

}  // namespace elog
