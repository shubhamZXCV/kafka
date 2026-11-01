#include "elog/storage/segment.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <system_error>

#include "elog/common/logging.h"
#include "elog/common/time.h"
#include "elog/protocol/record_batch.h"

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
    const std::string indexPath = dir + "/" + indexFileName(baseOffset);
    auto index = SparseIndex::open(indexPath, indexIntervalBytes);
    bool rebuild = false;
    if (!index.ok() && index.error() == ErrorCode::CORRUPT_MESSAGE) {
        std::filesystem::remove(indexPath);  // LLD §5.6: an invalid index is rebuilt
        index = SparseIndex::open(indexPath, indexIntervalBytes);
        rebuild = true;
    }
    if (!index.ok()) {
        ::close(fd);
        return {index.error(), index.message()};
    }
    const auto size = static_cast<std::uint32_t>(st.st_size);
    auto segment = std::shared_ptr<Segment>(new Segment(
        fd, baseOffset, std::move(path), std::move(index).value(), time::monotonicMs(), size));
    // Also rebuilt: an index pointing past the log, or none at all for a non-empty log
    // (deleted, or lost before it was flushed).
    segment->observeTimestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::seconds(st.st_mtim.tv_sec))
                                  .count());
    if (rebuild || !segment->index().fitsLog(size) ||
        (size > 0 && segment->index().entryCount() == 0)) {
        if (auto status = segment->rebuildIndex(); !status.ok()) {
            return {status.error(), status.message()};
        }
    }
    return segment;
}

// Walks the batch headers and re-adds entries exactly as appends would have. Stops at the
// first header that does not fit; the torn-tail scan that follows deals with it.
Status Segment::rebuildIndex() {
    if (auto status = index_->truncateAfterPos(0); !status.ok()) {
        return status;
    }
    std::vector<std::byte> header;
    std::uint32_t pos = 0;
    const std::uint32_t end = size();
    while (end - pos >= batch::kHeaderBytes) {
        if (auto status = readAt(pos, batch::kHeaderBytes, header); !status.ok()) {
            return status;
        }
        const RecordBatchHeader h = readBatchHeader(header);
        const std::uint64_t total = batch::kRecordBatchOverhead + h.batchLength;
        if (total > end - pos || h.baseOffset < baseOffset_) {
            break;
        }
        const auto relative = static_cast<std::uint32_t>(h.baseOffset - baseOffset_);
        if (auto status = index_->maybeAppend(relative, pos, static_cast<std::uint32_t>(total));
            !status.ok()) {
            return status;
        }
        pos += static_cast<std::uint32_t>(total);
    }
    logging::get("storage")->info("index_rebuilt segment={} entries={}", path_,
                                  index_->entryCount());
    return okStatus();  // no sync: an index can always be rebuilt, and log flushes sync it
}

void Segment::observeTimestamp(TimeMs ts) {
    if (ts > maxTimestampMs_.load(std::memory_order_relaxed)) {
        maxTimestampMs_.store(ts, std::memory_order_relaxed);  // only the appender writes
    }
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
