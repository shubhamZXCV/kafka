#include "elog/broker/request_dispatcher.h"

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/json_codec.h"
#include "elog/protocol/messages.h"
#include "elog/protocol/msgpack_codec.h"

namespace elog {

RequestDispatcher::RequestDispatcher(ProduceHandler& produce, FetchHandler& fetch,
                                     ReplicaFetchHandler& replicaFetch, ReplicaManager& replicas,
                                     BrokerId self)
    : produce_(produce),
      fetch_(fetch),
      replicaFetch_(replicaFetch),
      replicas_(replicas),
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
    return encode(replicas_.applyLeaderAndIsr(leaderAndIsr.value(), self_), request.correlationId);
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

}  // namespace elog
