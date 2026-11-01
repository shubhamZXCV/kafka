#include "elog/coordinator/broker_liveness.h"

#include "elog/common/logging.h"
#include "elog/protocol/error_response.h"
#include "elog/protocol/json_codec.h"

namespace elog {

BrokerLiveness::BrokerLiveness(Repository& repo, std::chrono::milliseconds sessionTimeout)
    : repo_(repo), sessionTimeout_(sessionTimeout) {}

// LLD §7.5: a (re-)registration bumps the persisted incarnation, so messages from the
// broker's previous life are recognisably stale.
Frame BrokerLiveness::onRegister(const Frame& request, TimeMs now) {
    auto registration = decodeRegisterBrokerRequest(request);
    if (!registration.ok()) {
        return encodeErrorResponse(request, registration.error());
    }
    const RegisterBrokerRequest& r = registration.value();
    if (r.brokerId < 0 || r.host.empty() || r.port == 0) {
        return encode(RegisterBrokerResponse{ErrorCode::INVALID_REQUEST, 0}, request.correlationId);
    }
    auto incarnation = repo_.registerBroker(r.brokerId, r.host, r.port);
    if (!incarnation.ok()) {
        logging::get("coordinator")
            ->critical("register_broker_failed broker={} error=\"{}\"", r.brokerId,
                       incarnation.message());
        return encode(RegisterBrokerResponse{ErrorCode::UNKNOWN_SERVER_ERROR, 0},
                      request.correlationId);
    }
    brokers_[r.brokerId] = {{r.brokerId, r.host, r.port}, incarnation.value(), true, now};
    logging::get("coordinator")
        ->info("broker_registered broker={} host={} port={} incarnation={}", r.brokerId, r.host,
               r.port, incarnation.value());
    return encode(RegisterBrokerResponse{ErrorCode::NONE, incarnation.value()},
                  request.correlationId);
}

// R-15: a dead, unknown or previous-incarnation broker must register again.
Frame BrokerLiveness::onHeartbeat(const Frame& request, TimeMs now) {
    auto heartbeat = decodeBrokerHeartbeatRequest(request);
    if (!heartbeat.ok()) {
        return encodeErrorResponse(request, heartbeat.error());
    }
    const auto it = brokers_.find(heartbeat.value().brokerId);
    if (it == brokers_.end() || !it->second.alive ||
        it->second.incarnation != heartbeat.value().incarnation) {
        return encode(BrokerHeartbeatResponse{ErrorCode::STALE_BROKER_INCARNATION},
                      request.correlationId);
    }
    it->second.lastHeartbeatMs = now;
    return encode(BrokerHeartbeatResponse{ErrorCode::NONE}, request.correlationId);
}

std::vector<BrokerId> BrokerLiveness::onTick(TimeMs now) {
    std::vector<BrokerId> dead;
    for (auto& [id, broker] : brokers_) {
        if (broker.alive && now - broker.lastHeartbeatMs > sessionTimeout_.count()) {
            broker.alive = false;
            dead.push_back(id);
            logging::get("coordinator")
                ->warn("broker_dead broker={} incarnation={} silent_ms={}", id, broker.incarnation,
                       now - broker.lastHeartbeatMs);
        }
    }
    return dead;
}

std::vector<BrokerAddress> BrokerLiveness::liveBrokers() const {
    std::vector<BrokerAddress> live;
    for (const auto& [id, broker] : brokers_) {
        if (broker.alive) {
            live.push_back(broker.address);
        }
    }
    return live;
}

}  // namespace elog
