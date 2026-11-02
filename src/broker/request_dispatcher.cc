#include "elog/broker/request_dispatcher.h"

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/json_codec.h"
#include "elog/protocol/messages.h"
#include "elog/protocol/msgpack_codec.h"
#include "elog/protocol/replication_codec.h"

namespace elog {

RequestDispatcher::RequestDispatcher(ProduceHandler& produce, FetchHandler& fetch,
                                     ReplicaFetchHandler& replicaFetch, ReplicaManager& replicas,
                                     ReplicaFetcherManager& fetchers, BrokerId self)
    : produce_(produce),
      fetch_(fetch),
      replicaFetch_(replicaFetch),
      replicas_(replicas),
      fetchers_(fetchers),
      self_(self) {}

Frame RequestDispatcher::dispatch(const Frame& request) {
    switch (request.apiKey) {
        case ApiKey::Produce:
            return handleProduce(request);
        case ApiKey::Fetch:
            return handleFetch(request);
        case ApiKey::LeaderAndIsr:
            return handleLeaderAndIsr(request);
        case ApiKey::ReplicaFetch:
            return handleReplicaFetch(request);
        case ApiKey::OffsetsForLeaderEpoch:
            return handleOffsetsForLeaderEpoch(request);
        default:
            break;
    }
    logging::get("broker")->warn("unhandled_request api={} correlation={}",
                                 apiKeyName(request.apiKey), request.correlationId);
    return encodeErrorResponse(request, ErrorCode::INVALID_REQUEST);
}

// Parse → act → reply. A body that does not decode gets INVALID_REQUEST; the frame itself
// was valid, so the connection stays open (R-10).
Frame RequestDispatcher::handleProduce(const Frame& request) {
    auto produce = decodeProduceRequest(request);
    if (!produce.ok()) {
        logging::get("broker")->warn("bad_request api=Produce correlation={} error=\"{}\"",
                                     request.correlationId, produce.message());
        return encodeErrorResponse(request, produce.error());
    }
    return encode(produce_.handle(produce.value()), request.correlationId);
}

Frame RequestDispatcher::handleFetch(const Frame& request) {
    auto fetch = decodeFetchRequest(request);
    if (!fetch.ok()) {
        logging::get("broker")->warn("bad_request api=Fetch correlation={} error=\"{}\"",
                                     request.correlationId, fetch.message());
        return encodeErrorResponse(request, fetch.error());
    }
    return encode(fetch_.handle(fetch.value()), request.correlationId);
}

// LLD §6.3: only the coordinator sends this; each partition's epoch check fences pushes
// that were delayed past a newer one.
Frame RequestDispatcher::handleLeaderAndIsr(const Frame& request) {
    auto leaderAndIsr = decodeLeaderAndIsrRequest(request);
    if (!leaderAndIsr.ok()) {
        logging::get("broker")->warn("bad_request api=LeaderAndIsr correlation={} error=\"{}\"",
                                     request.correlationId, leaderAndIsr.message());
        return encodeErrorResponse(request, leaderAndIsr.error());
    }
    const LeaderAndIsrResponse response = replicas_.applyLeaderAndIsr(leaderAndIsr.value(), self_);
    fetchers_.reconcile(leaderAndIsr.value().brokers);  // LLD §6.3: follow the new leaders
    return encode(response, request.correlationId);
}

Frame RequestDispatcher::handleReplicaFetch(const Frame& request) {
    auto fetch = decodeReplicaFetchRequest(request);
    if (!fetch.ok()) {
        logging::get("broker")->warn("bad_request api=ReplicaFetch correlation={} error=\"{}\"",
                                     request.correlationId, fetch.message());
        return encodeErrorResponse(request, fetch.error());
    }
    return encode(replicaFetch_.handle(fetch.value()), request.correlationId);
}

// LLD §6.8, leader side: where each asked-about epoch ends in our log.
Frame RequestDispatcher::handleOffsetsForLeaderEpoch(const Frame& request) {
    auto query = decodeOffsetsForLeaderEpochRequest(request);
    if (!query.ok()) {
        return encodeErrorResponse(request, query.error());
    }
    OffsetsForLeaderEpochResponse response;
    for (const EpochQuery& q : query.value().partitions) {
        EpochEndOffset out{q.topic, q.partition, ErrorCode::NONE, 0, 0};
        const auto replica = replicas_.get({q.topic, q.partition});
        auto end = replica
                       ? replica->endOffsetForEpoch(q.leaderEpoch)
                       : Result<std::pair<Epoch, Offset>>(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION);
        if (end.ok()) {
            out.leaderEpoch = end.value().first;
            out.endOffset = end.value().second;
        } else {
            out.error = end.error();
        }
        response.partitions.push_back(out);
    }
    return encode(response, request.correlationId);
}

}  // namespace elog
