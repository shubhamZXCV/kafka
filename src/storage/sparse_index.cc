#include "elog/storage/sparse_index.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <system_error>

#include "elog/common/bytes.h"

namespace elog {

namespace {

constexpr std::size_t kEntryBytes = 8;

Status ioError(const std::string& what, const std::string& path, int err) {
    return {ErrorCode::UNKNOWN_SERVER_ERROR,
            what + " " + path + ": " + std::system_category().message(err)};
}

Result<std::vector<IndexEntry>> readEntries(int fd, const std::string& path) {
    struct stat st {};
    if (::fstat(fd, &st) != 0) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, ioError("stat", path, errno).message()};
    }
    if (st.st_size % kEntryBytes != 0) {
        return {ErrorCode::CORRUPT_MESSAGE, path + ": length is not a multiple of 8"};
    }
    std::vector<std::byte> raw(static_cast<std::size_t>(st.st_size));
    if (!raw.empty() && ::pread(fd, raw.data(), raw.size(), 0) != st.st_size) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, path + ": short read"};
    }
    std::vector<IndexEntry> entries;
    for (std::size_t at = 0; at < raw.size(); at += kEntryBytes) {
        const IndexEntry e{loadBE<std::uint32_t>(raw, at), loadBE<std::uint32_t>(raw, at + 4)};
        if (!entries.empty() && (e.relativeOffset <= entries.back().relativeOffset ||
                                 e.position <= entries.back().position)) {
            return {ErrorCode::CORRUPT_MESSAGE, path + ": entries not strictly increasing"};
        }
        entries.push_back(e);
    }
    return entries;
}

}  // namespace

Result<std::unique_ptr<SparseIndex>> SparseIndex::open(const std::string& path,
                                                       std::uint32_t intervalBytes) {
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        const auto status = ioError("open index", path, errno);
        return {status.error(), status.message()};
    }
    auto entries = readEntries(fd, path);
    if (!entries.ok()) {
        ::close(fd);
        return {entries.error(), entries.message()};
    }
    return std::unique_ptr<SparseIndex>(
        new SparseIndex(fd, path, intervalBytes, std::move(entries).value()));
}

SparseIndex::~SparseIndex() {
    ::close(fd_);
}

Status SparseIndex::maybeAppend(std::uint32_t relativeOffset, std::uint32_t position,
                                std::uint32_t batchBytes) {
    std::lock_guard lock(mu_);  // leaf lock (R-19)
    if (entries_.empty() || bytesSinceLast_ >= intervalBytes_) {
        std::vector<std::byte> raw(kEntryBytes);
        storeBE<std::uint32_t>(raw, 0, relativeOffset);
        storeBE<std::uint32_t>(raw, 4, position);
        const auto at = static_cast<off_t>(entries_.size() * kEntryBytes);
        if (::pwrite(fd_, raw.data(), raw.size(), at) != static_cast<ssize_t>(raw.size())) {
            return ioError("write index", path_, errno);
        }
        entries_.push_back({relativeOffset, position});
        bytesSinceLast_ = 0;
    }
    bytesSinceLast_ += batchBytes;
    return okStatus();
}

IndexEntry SparseIndex::floor(std::uint32_t relativeOffset) const {
    std::lock_guard lock(mu_);  // leaf lock (R-19)
    const auto after = std::upper_bound(
        entries_.begin(), entries_.end(), relativeOffset,
        [](std::uint32_t target, const IndexEntry& e) { return target < e.relativeOffset; });
    return after == entries_.begin() ? IndexEntry{} : *std::prev(after);
}

Status SparseIndex::truncateAfterPos(std::uint32_t position) {
    std::lock_guard lock(mu_);  // leaf lock (R-19)
    std::erase_if(entries_, [&](const IndexEntry& e) { return e.position >= position; });
    if (::ftruncate(fd_, static_cast<off_t>(entries_.size() * kEntryBytes)) != 0) {
        return ioError("truncate index", path_, errno);
    }
    bytesSinceLast_ = 0;
    return okStatus();
}

bool SparseIndex::fitsLog(std::uint32_t logSize) const {
    std::lock_guard lock(mu_);  // leaf lock (R-19)
    return entries_.empty() || entries_.back().position < logSize;
}

Status SparseIndex::flush() {
    if (::fdatasync(fd_) != 0) {
        return ioError("fdatasync index", path_, errno);
    }
    return okStatus();
}

std::size_t SparseIndex::entryCount() const {
    std::lock_guard lock(mu_);  // leaf lock (R-19)
    return entries_.size();
}

}  // namespace elog
