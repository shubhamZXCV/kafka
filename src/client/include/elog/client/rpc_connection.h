#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include "elog/common/result.h"
#include "elog/common/socket.h"
#include "elog/protocol/frame.h"

namespace elog {

// Splits "host:port". INVALID_REQUEST if the port is missing or not a number in [1, 65535].
Result<std::pair<std::string, std::uint16_t>> parseHostPort(const std::string& address);

// One request/response connection to a broker or the coordinator (LLD §4.2: one request
// in flight). It connects on first use, stamps each request with the next correlation ID
// and checks that the response carries the same ID and API key. Any failure drops the
// connection; the next call reconnects.
//
// Thread rules: one thread at a time (each client thread owns its connections).
class RpcConnection {
public:
    RpcConnection(std::string host, std::uint16_t port,
                  std::chrono::milliseconds timeout = std::chrono::milliseconds(30'000));

    // Sends `request` (its correlation ID is replaced) and returns the response.
    Result<Frame> call(Frame request);

    const std::string& host() const { return host_; }
    std::uint16_t port() const { return port_; }

private:
    Status connect();

    std::string host_;
    std::uint16_t port_;
    std::chrono::milliseconds timeout_;  // connect, send and receive
    std::optional<Socket> socket_;
    std::uint32_t nextCorrelationId_ = 1;
};

}  // namespace elog
