#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <vector>

#include "elog/common/types.h"
#include "elog/coordinator/repository.h"
#include "elog/protocol/frame.h"
#include "elog/protocol/messages.h"

namespace elog {

// Which brokers are registered and alive (LLD §7.5): RegisterBroker, BrokerHeartbeat, and
// a broker is declared dead once it misses heartbeats for broker.session.timeout.ms.
//
// Thread rules: event-loop thread only (LLD §11.3).
class BrokerLiveness {
public:
    BrokerLiveness(Repository& repo, std::chrono::milliseconds sessionTimeout);

    Frame onRegister(const Frame& request, TimeMs now);
    Frame onHeartbeat(const Frame& request, TimeMs now);

    // Marks brokers whose session expired as dead and returns them.
    std::vector<BrokerId> onTick(TimeMs now);

    std::vector<BrokerAddress> liveBrokers() const;

private:
    struct BrokerState {
        BrokerAddress address;
        std::uint32_t incarnation = 0;
        bool alive = false;
        TimeMs lastHeartbeatMs = 0;
    };

    Repository& repo_;
    const std::chrono::milliseconds sessionTimeout_;
    std::map<BrokerId, BrokerState> brokers_;
};

}  // namespace elog
