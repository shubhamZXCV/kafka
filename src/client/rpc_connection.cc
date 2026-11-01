#include "elog/client/rpc_connection.h"

#include <charconv>
#include <utility>

namespace elog {

namespace {

constexpr std::size_t kMaxResponseBytes = 64 << 20;  // a fetch response can be large

}  // namespace

Result<std::pair<std::string, std::uint16_t>> parseHostPort(const std::string& address) {
    const auto colon = address.rfind(':');
    std::uint16_t port = 0;
    if (colon == std::string::npos || colon == 0) {
        return {ErrorCode::INVALID_REQUEST, "expected host:port, got '" + address + "'"};
    }
    const char* end = address.data() + address.size();
    const auto [ptr, ec] = std::from_chars(address.data() + colon + 1, end, port);
    if (ec != std::errc() || ptr != end || port == 0) {
        return {ErrorCode::INVALID_REQUEST, "bad port in '" + address + "'"};
    }
    return std::pair{address.substr(0, colon), port};
}

RpcConnection::RpcConnection(std::string host, std::uint16_t port,
                             std::chrono::milliseconds timeout)
    : host_(std::move(host)), port_(port), timeout_(timeout) {}

Status RpcConnection::connect() {
    auto socket = Socket::connect(host_, port_, timeout_);
    if (!socket.ok()) {
        return {socket.error(), socket.message()};
    }
    if (auto status = socket.value().setTimeouts(timeout_, timeout_); !status.ok()) {
        return status;
    }
    socket_.emplace(std::move(socket).value());
    return okStatus();
}

Result<Frame> RpcConnection::call(Frame request) {
    if (!socket_) {
        if (auto status = connect(); !status.ok()) {
            return {status.error(), status.message()};
        }
    }
    request.correlationId = nextCorrelationId_++;
    if (auto status = writeFrame(*socket_, request); !status.ok()) {
        socket_.reset();
        return {status.error(), status.message()};
    }
    auto response = readFrame(*socket_, kMaxResponseBytes);
    if (!response.ok()) {
        socket_.reset();
        return response;
    }
    // A mismatch means the stream is out of step; nothing later on it can be trusted.
    const Frame& r = response.value();
    if (r.correlationId != request.correlationId || r.apiKey != request.apiKey || !r.isResponse()) {
        socket_.reset();
        return {ErrorCode::CORRUPT_MESSAGE,
                "response does not match request " + std::to_string(request.correlationId)};
    }
    return response;
}

}  // namespace elog
