#include "elog/common/socket.h"

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <memory>
#include <system_error>
#include <utility>

namespace elog {

namespace {

using AddrList = std::unique_ptr<addrinfo, decltype(&freeaddrinfo)>;

std::string errnoMessage(const std::string& what, int err) {
    return what + ": " + std::system_category().message(err);
}

std::string endpoint(const std::string& host, std::uint16_t port) {
    return host + ":" + std::to_string(port);
}

Result<AddrList> resolve(const std::string& host, std::uint16_t port, bool passive) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = passive ? AI_PASSIVE : 0;
    addrinfo* list = nullptr;
    const int rc = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &list);
    if (rc != 0) {
        return {ErrorCode::NETWORK_ERROR,
                "resolve " + endpoint(host, port) + ": " + gai_strerror(rc)};
    }
    return AddrList(list, &freeaddrinfo);
}

// Request/response traffic is small and latency-bound, so turn off Nagle's algorithm.
void setNoDelay(int fd) {
    const int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

// Connects `fd` with a deadline: a non-blocking connect, then poll for completion.
// Returns 0 on success or an errno value.
int connectWithTimeout(int fd, const addrinfo& addr, std::chrono::milliseconds timeout) {
    const int flags = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int err = 0;
    if (::connect(fd, addr.ai_addr, addr.ai_addrlen) != 0) {
        err = errno;
    }
    if (err == EINPROGRESS) {
        pollfd pfd{.fd = fd, .events = POLLOUT, .revents = 0};
        const int ready = poll(&pfd, 1, static_cast<int>(timeout.count()));
        if (ready == 0) {
            err = ETIMEDOUT;
        } else if (ready < 0) {
            err = errno;
        } else {
            socklen_t len = sizeof(err);
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        }
    }
    fcntl(fd, F_SETFL, flags);  // back to blocking
    return err;
}

timeval toTimeval(std::chrono::milliseconds ms) {
    return timeval{.tv_sec = static_cast<time_t>(ms.count() / 1000),
                   .tv_usec = static_cast<suseconds_t>((ms.count() % 1000) * 1000)};
}

}  // namespace

// --- Socket ---

Result<Socket> Socket::connect(const std::string& host, std::uint16_t port,
                               std::chrono::milliseconds timeout) {
    auto addrs = resolve(host, port, /*passive=*/false);
    if (!addrs.ok()) {
        return {addrs.error(), addrs.message()};
    }
    int lastErr = ECONNREFUSED;
    // A name may resolve to several addresses (e.g. IPv6 and IPv4); use the first that works.
    for (const addrinfo* a = addrs.value().get(); a != nullptr; a = a->ai_next) {
        const int fd = ::socket(a->ai_family, a->ai_socktype | SOCK_CLOEXEC, a->ai_protocol);
        if (fd < 0) {
            lastErr = errno;
            continue;
        }
        lastErr = connectWithTimeout(fd, *a, timeout);
        if (lastErr == 0) {
            setNoDelay(fd);
            return Socket(fd);
        }
        ::close(fd);
    }
    return {ErrorCode::NETWORK_ERROR, errnoMessage("connect " + endpoint(host, port), lastErr)};
}

Socket::Socket(Socket&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

Socket::~Socket() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Status Socket::setTimeouts(std::chrono::milliseconds rcv, std::chrono::milliseconds snd) {
    const timeval rcvTv = toTimeval(rcv);
    const timeval sndTv = toTimeval(snd);
    if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &rcvTv, sizeof(rcvTv)) != 0 ||
        setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &sndTv, sizeof(sndTv)) != 0) {
        return {ErrorCode::NETWORK_ERROR, errnoMessage("set socket timeouts", errno)};
    }
    return okStatus();
}

Status Socket::readExact(std::span<std::byte> out) {
    std::size_t done = 0;
    while (done < out.size()) {
        const ssize_t n = ::recv(fd_, out.data() + done, out.size() - done, 0);
        if (n > 0) {
            done += static_cast<std::size_t>(n);
        } else if (n == 0) {
            return {ErrorCode::NETWORK_ERROR, "connection closed by peer"};
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return {ErrorCode::NETWORK_ERROR, "read timed out"};
        } else if (errno != EINTR) {
            return {ErrorCode::NETWORK_ERROR, errnoMessage("read", errno)};
        }
    }
    return okStatus();
}

Status Socket::writeAll(std::span<const std::byte> in) {
    std::size_t done = 0;
    while (done < in.size()) {
        // MSG_NOSIGNAL: a closed peer gives EPIPE instead of killing the process with SIGPIPE.
        const ssize_t n = ::send(fd_, in.data() + done, in.size() - done, MSG_NOSIGNAL);
        if (n >= 0) {
            done += static_cast<std::size_t>(n);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return {ErrorCode::NETWORK_ERROR, "write timed out"};
        } else if (errno != EINTR) {
            return {ErrorCode::NETWORK_ERROR, errnoMessage("write", errno)};
        }
    }
    return okStatus();
}

void Socket::shutdown() noexcept {
    if (fd_ >= 0) {
        ::shutdown(fd_, SHUT_RDWR);
    }
}

// --- Listener ---

Result<Listener> Listener::bind(const std::string& host, std::uint16_t port, int backlog) {
    auto addrs = resolve(host, port, /*passive=*/true);
    if (!addrs.ok()) {
        return {addrs.error(), addrs.message()};
    }
    const addrinfo& a = *addrs.value();
    const int fd = ::socket(a.ai_family, a.ai_socktype | SOCK_CLOEXEC, a.ai_protocol);
    if (fd < 0) {
        return {ErrorCode::NETWORK_ERROR, errnoMessage("socket", errno)};
    }
    Listener listener(fd);  // closes fd on every early return below
    // A restarted broker must be able to rebind its port while old connections linger.
    const int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (::bind(fd, a.ai_addr, a.ai_addrlen) != 0 || ::listen(fd, backlog) != 0) {
        return {ErrorCode::NETWORK_ERROR, errnoMessage("listen on " + endpoint(host, port), errno)};
    }
    return listener;
}

Listener::Listener(Listener&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

Listener& Listener::operator=(Listener&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

Listener::~Listener() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Result<Socket> Listener::accept() {
    while (true) {
        const int fd = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC);
        if (fd >= 0) {
            setNoDelay(fd);
            return Socket(fd);
        }
        if (errno != EINTR) {
            return {ErrorCode::NETWORK_ERROR, errnoMessage("accept", errno)};
        }
    }
}

}  // namespace elog
