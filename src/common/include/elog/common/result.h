#pragma once

#include <cassert>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "elog/common/error_code.h"

namespace elog {

// Either a value or an error code with a message. This is how every fallible function in
// the project reports failure; exceptions never cross a module boundary (implementation
// plan §4). A minimal stand-in for C++23 std::expected.
//
//     Result<Socket> sock = Socket::connect(host, port, timeout);
//     if (!sock.ok()) {
//         return {sock.error(), "connect to coordinator: " + sock.message()};
//     }
//     useSocket(sock.value());
template <class T>
class Result {
public:
    Result(T value) : value_(std::move(value)) {}

    Result(ErrorCode code, std::string message = {}) : code_(code), message_(std::move(message)) {
        assert(code != ErrorCode::NONE && "an error Result needs a real error code");
    }

    bool ok() const noexcept { return code_ == ErrorCode::NONE; }

    // Calling value() on an error is a bug; check ok() first.
    const T& value() const& {
        assert(ok());
        return *value_;
    }
    T& value() & {
        assert(ok());
        return *value_;
    }
    T&& value() && {
        assert(ok());
        return std::move(*value_);
    }

    ErrorCode error() const noexcept { return code_; }
    const std::string& message() const noexcept { return message_; }

private:
    std::optional<T> value_;  // set exactly when ok()
    ErrorCode code_ = ErrorCode::NONE;
    std::string message_;
};

// Result of an operation that returns nothing on success.
using Status = Result<std::monostate>;

inline Status okStatus() {
    return std::monostate{};
}

}  // namespace elog
