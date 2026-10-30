#include "elog/storage/segment.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <system_error>

namespace elog {

namespace {

Status ioError(const std::string& what, const std::string& path, int err) {
    return {ErrorCode::UNKNOWN_SERVER_ERROR,
            what + " " + path + ": " + std::system_category().message(err)};
}

}  // namespace

std::string segmentFileName(Offset baseOffset) {
    char name[32];
    std::snprintf(name, sizeof(name), "%020llu.log", static_cast<unsigned long long>(baseOffset));
    return name;
}

Result<std::shared_ptr<Segment>> Segment::create(const std::string& dir, Offset baseOffset) {
    const std::string path = dir + "/" + segmentFileName(baseOffset);
    // O_EXCL: never silently overwrite an existing segment (and the data in it).
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) {
        const auto status = ioError("create segment", path, errno);
        return {status.error(), status.message()};
    }
    return std::shared_ptr<Segment>(new Segment(fd, baseOffset, path));
}

Segment::~Segment() {
    ::close(fd_);
}

Status Segment::append(std::span<const std::byte> bytes) {
    const std::uint32_t start = size_.load(std::memory_order_relaxed);  // only we write it
    // Positions are u32 (LLD §5.2.2). Rolling to a new segment normally prevents this.
    if (bytes.size() > UINT32_MAX - start) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, "segment " + path_ + " is full"};
    }
    std::size_t done = 0;
    while (done < bytes.size()) {
        const ssize_t n = ::pwrite(fd_, bytes.data() + done, bytes.size() - done,
                                   static_cast<off_t>(start + done));
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            // Bytes past size_ are invisible to readers and overwritten by the next append,
            // so a failed write leaves the segment consistent.
            return ioError("write segment", path_, errno);
        }
        done += static_cast<std::size_t>(n);
    }
    // LLD §11.4: publish the new size only after the bytes are written.
    size_.store(static_cast<std::uint32_t>(start + bytes.size()), std::memory_order_release);
    return okStatus();
}

Status Segment::readAt(std::uint64_t pos, std::size_t len, std::vector<std::byte>& out) const {
    out.resize(len);
    std::size_t done = 0;
    while (done < len) {
        const ssize_t n =
            ::pread(fd_, out.data() + done, len - done, static_cast<off_t>(pos + done));
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0) {
            return ioError("read segment", path_, errno);
        }
        if (n == 0) {
            return {ErrorCode::UNKNOWN_SERVER_ERROR,
                    "read segment " + path_ + ": past end of file"};
        }
        done += static_cast<std::size_t>(n);
    }
    return okStatus();
}

Status Segment::flush() {
    if (::fdatasync(fd_) != 0) {
        return ioError("fdatasync segment", path_, errno);
    }
    return okStatus();
}

}  // namespace elog
