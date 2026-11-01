#include "elog/storage/segment.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <system_error>

#include "elog/common/time.h"

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

std::string indexFileName(Offset baseOffset) {
    char name[32];
    std::snprintf(name, sizeof(name), "%020llu.index", static_cast<unsigned long long>(baseOffset));
    return name;
}

Result<std::shared_ptr<Segment>> Segment::create(const std::string& dir, Offset baseOffset,
                                                 std::uint32_t indexIntervalBytes) {
    const std::string path = dir + "/" + segmentFileName(baseOffset);
    // O_EXCL: never silently overwrite an existing segment (and the data in it).
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) {
        const auto status = ioError("create segment", path, errno);
        return {status.error(), status.message()};
    }
    return fromFd(fd, dir, baseOffset, path, indexIntervalBytes);
}

Result<std::shared_ptr<Segment>> Segment::open(const std::string& dir, Offset baseOffset,
                                               std::uint32_t indexIntervalBytes) {
    const std::string path = dir + "/" + segmentFileName(baseOffset);
    const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        const auto status = ioError("open segment", path, errno);
        return {status.error(), status.message()};
    }
    return fromFd(fd, dir, baseOffset, path, indexIntervalBytes);
}

Result<std::shared_ptr<Segment>> Segment::fromFd(int fd, const std::string& dir, Offset baseOffset,
                                                 std::string path,
                                                 std::uint32_t indexIntervalBytes) {
    struct stat st {};
    if (::fstat(fd, &st) != 0 || st.st_size > UINT32_MAX) {
        ::close(fd);
        return {ErrorCode::UNKNOWN_SERVER_ERROR, "stat segment " + path + " failed or too large"};
    }
    auto index = SparseIndex::open(dir + "/" + indexFileName(baseOffset), indexIntervalBytes);
    if (!index.ok()) {
        ::close(fd);
        return {index.error(), index.message()};
    }
    return std::shared_ptr<Segment>(new Segment(fd, baseOffset, std::move(path),
                                                std::move(index).value(), time::monotonicMs(),
                                                static_cast<std::uint32_t>(st.st_size)));
}

std::string Segment::indexPath() const {
    return path_.substr(0, path_.size() - 4) + ".index";  // NNN.log -> NNN.index
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

Status Segment::truncateToPos(std::uint32_t pos) {
    if (::ftruncate(fd_, static_cast<off_t>(pos)) != 0) {
        return ioError("truncate segment", path_, errno);
    }
    size_.store(pos, std::memory_order_release);
    return flush();
}

Status Segment::flush() {
    if (::fdatasync(fd_) != 0) {
        return ioError("fdatasync segment", path_, errno);
    }
    return okStatus();
}

}  // namespace elog
