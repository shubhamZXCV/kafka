#include "elog/coordinator/offset_manager.h"

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/group_codec.h"

namespace elog {

OffsetManager::OffsetManager(Repository& repo, EventLog& events) : repo_(repo), events_(events) {}

// LLD §7.4: every partition of one request in one transaction (one fsync per request).
// TODO(T4.1): commits are unfenced until the ownership-epoch CAS lands.
Frame OffsetManager::onCommit(const Frame& request, TimeMs nowWallMs) {
    auto commit = decodeOffsetCommitRequest(request);
    if (!commit.ok()) {
        return encodeErrorResponse(request, commit.error());
    }
    const OffsetCommitRequest& r = commit.value();
    OffsetCommitResponse response;
    auto txn = repo_.begin();
    Status status = txn.ok() ? okStatus() : Status{txn.error(), txn.message()};
    for (const OffsetCommitEntry& e : r.offsets) {
        if (status.ok()) {
            status = repo_.commitOffsetUnfenced(r.groupId, {e.topic, e.partition}, e.offset,
                                                e.epoch, nowWallMs);
        }
        response.offsets.push_back({e.topic, e.partition, ErrorCode::NONE});
    }
    if (status.ok()) {
        status = txn.value().commit();
    }
    if (!status.ok()) {  // nothing was stored (the transaction rolled back)
        logging::get("coordinator")
            ->critical("offset_commit_failed group={} error=\"{}\"", r.groupId, status.message());
        for (auto& e : response.offsets) {
            e.error = ErrorCode::UNKNOWN_SERVER_ERROR;
        }
        return encode(response, request.correlationId);
    }
    for (const OffsetCommitEntry& e : r.offsets) {
        events_.emit({.event = "commit_accepted",
                      .group = r.groupId,
                      .tp = TopicPartition{e.topic, e.partition},
                      .epoch = e.epoch,
                      .member = r.memberId,
                      .offset = e.offset});
    }
    return encode(response, request.correlationId);
}

Frame OffsetManager::onFetch(const Frame& request) {
    auto fetch = decodeOffsetFetchRequest(request);
    if (!fetch.ok()) {
        return encodeErrorResponse(request, fetch.error());
    }
    OffsetFetchResponse response;
    for (const TopicPartition& tp : fetch.value().partitions) {
        auto offset = repo_.committedOffset(fetch.value().groupId, tp);
        if (!offset.ok()) {
            response.offsets.push_back(
                {tp.topic, tp.partition, -1, ErrorCode::UNKNOWN_SERVER_ERROR});
        } else {
            const auto value = offset.value() ? static_cast<std::int64_t>(*offset.value()) : -1;
            response.offsets.push_back({tp.topic, tp.partition, value, ErrorCode::NONE});
        }
    }
    return encode(response, request.correlationId);
}

}  // namespace elog
