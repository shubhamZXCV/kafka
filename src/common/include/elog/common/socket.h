#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

#include "elog/common/result.h"

namespace elog {

// A connected TCP socket that owns its file descriptor (LLD §3.3). Move-only; closed on
// destruction. Every failure (connect refused, peer closed, timeout) is ErrorCode::NETWORK_ERROR.
//
// Thread rules: one thread reads and one thread writes at a time. shutdown() may be called
// from any thread to unblock a reader or writer.
class Socket {
public:
    static Result<Socket> connect(const std::string& host, std::uint16_t port,
                                  std::chrono::milliseconds timeout);

    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    ~Socket();

    // A blocked read or write fails with NETWORK_ERROR after this long. Zero means no limit.
    Status setTimeouts(std::chrono::milliseconds rcv, std::chrono::milliseconds snd);

    // Fills `out` completely, or fails. Loops over short reads and EINTR.
    Status readExact(std::span<std::byte> out);
    // Sends all of `in`, or fails. Loops over short writes and EINTR.
    Status writeAll(std::span<const std::byte> in);

    // Unblocks any thread inside readExact/writeAll; later calls fail.
    void shutdown() noexcept;

private:
    explicit Socket(int fd) : fd_(fd) {}
    friend class Listener;

    int fd_ = -1;
};

// A listening TCP socket. Move-only; closed on destruction.
class Listener {
public:
    static Result<Listener> bind(const std::string& host, std::uint16_t port, int backlog = 128);

    Listener(Listener&& other) noexcept;
    Listener& operator=(Listener&& other) noexcept;
    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;
    ~Listener();

    // Blocks until a client connects.
    Result<Socket> accept();

private:
    explicit Listener(int fd) : fd_(fd) {}

    int fd_ = -1;
};

}  // namespace elog
