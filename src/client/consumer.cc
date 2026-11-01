#include "elog/client/consumer.h"

#include <utility>

#include "elog/client/fetcher.h"
#include "elog/common/logging.h"
#include "elog/common/time.h"

namespace elog {

Consumer::Consumer(ConsumerConfig cfg, Metrics& metrics)
    : cfg_(std::move(cfg)),
      metrics_(metrics),
      metadata_(cfg_.coordinator),
      inflight_(cfg_.inflightMax),
      fetcher_(std::make_unique<Fetcher>(cfg_, metadata_, inflight_, metrics_)) {}

Consumer::~Consumer() {
    close();
    fetcher_->stop();
}

Status Consumer::run(const std::function<void(const ConsumedRecord&)>& process) {
    if (!cfg_.groupId.empty()) {
        return {ErrorCode::INVALID_REQUEST, "consumer groups are not supported yet"};
    }
    auto partitions = partitionsToRead();
    if (!partitions.ok()) {
        return {partitions.error(), partitions.message()};
    }
    logging::get("consumer")
        ->info("consuming mode=manual partitions={}", partitions.value().size());
    fetcher_->start(std::move(partitions).value());
    while (!closed_) {
        auto record = inflight_.pop(std::chrono::milliseconds(100));
        if (!record) {
            continue;
        }
        process(*record);
        const std::string labels =
            "topic=" + record->tp.topic + ";partition=" + std::to_string(record->tp.partition);
        metrics_.counter("consume_records_total", labels).add();
        metrics_.histogram("e2e_latency_ms")
            .record(static_cast<double>(time::wallMs() - record->timestampMs));
    }
    fetcher_->stop();
    return okStatus();
}

void Consumer::close() {
    closed_ = true;
}

// R-16: the configured partitions, or every partition of the configured topics. Either
// way their leaders are looked up now, before the first fetch.
Result<std::vector<TopicPartition>> Consumer::partitionsToRead() {
    std::vector<std::string> topics = cfg_.topics;
    for (const TopicPartition& tp : cfg_.assignment) {
        topics.push_back(tp.topic);
    }
    if (auto status = metadata_.refresh(topics); !status.ok()) {
        return {status.error(), status.message()};
    }
    if (!cfg_.assignment.empty()) {
        return cfg_.assignment;
    }
    std::vector<TopicPartition> partitions;
    for (const std::string& topic : cfg_.topics) {
        const auto count = metadata_.partitionCount(topic).value_or(0);
        for (std::uint32_t p = 0; p < count; ++p) {
            partitions.push_back({topic, p});
        }
    }
    if (partitions.empty()) {
        return {ErrorCode::UNKNOWN_TOPIC_OR_PARTITION, "nothing to consume"};
    }
    return partitions;
}

}  // namespace elog
