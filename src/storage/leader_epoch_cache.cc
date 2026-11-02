#include "elog/storage/leader_epoch_cache.h"

#include <algorithm>
#include <fstream>
#include <sstream>

#include "elog/storage/checkpoint_file.h"

namespace elog {

namespace {

constexpr int kVersion = 0;

}  // namespace

Result<std::unique_ptr<LeaderEpochCache>> LeaderEpochCache::open(const std::string& dir) {
    const std::string path = dir + "/leader-epoch.checkpoint";
    std::vector<EpochEntry> entries;
    std::ifstream in(path);
    if (in) {
        int version = -1;
        std::size_t count = 0;
        if (!(in >> version >> count) || version != kVersion) {
            return {ErrorCode::CORRUPT_MESSAGE, path + ": bad header"};
        }
        for (std::size_t i = 0; i < count; ++i) {
            EpochEntry e;
            if (!(in >> e.epoch >> e.startOffset) ||
                (!entries.empty() &&
                 (e.epoch <= entries.back().epoch || e.startOffset < entries.back().startOffset))) {
                return {ErrorCode::CORRUPT_MESSAGE, path + ": bad entry " + std::to_string(i)};
            }
            entries.push_back(e);
        }
    }
    return std::unique_ptr<LeaderEpochCache>(new LeaderEpochCache(path, std::move(entries)));
}

Status LeaderEpochCache::assign(Epoch epoch, Offset startOffset) {
    if (!entries_.empty() && entries_.back().epoch >= epoch) {
        return okStatus();  // already known (or older: a stale leader never writes here)
    }
    entries_.push_back({epoch, startOffset});
    return persist();
}

std::optional<Epoch> LeaderEpochCache::latestEpoch() const {
    if (entries_.empty()) {
        return std::nullopt;
    }
    return entries_.back().epoch;
}

std::optional<Offset> LeaderEpochCache::startOffsetOf(Epoch epoch) const {
    for (const EpochEntry& e : entries_) {
        if (e.epoch == epoch) {
            return e.startOffset;
        }
    }
    return std::nullopt;
}

std::pair<Epoch, Offset> LeaderEpochCache::endOffsetFor(Epoch epoch, Offset leo) const {
    const auto after =
        std::upper_bound(entries_.begin(), entries_.end(), epoch,
                         [](Epoch e, const EpochEntry& entry) { return e < entry.epoch; });
    if (after == entries_.begin()) {
        return {epoch, entries_.empty() ? Offset{0} : entries_.front().startOffset};
    }
    const Epoch found = std::prev(after)->epoch;
    return {found, after == entries_.end() ? leo : after->startOffset};
}

Status LeaderEpochCache::truncateFromEnd(Offset offset) {
    const auto before = entries_.size();
    std::erase_if(entries_, [&](const EpochEntry& e) { return e.startOffset >= offset; });
    return entries_.size() == before ? okStatus() : persist();
}

Status LeaderEpochCache::truncateFromStart(Offset offset) {
    // Keep the epoch in force at `offset` (moved to start there) and everything after it.
    std::size_t keep = 0;
    while (keep + 1 < entries_.size() && entries_[keep + 1].startOffset <= offset) {
        ++keep;
    }
    if (entries_.empty() || (keep == 0 && entries_[0].startOffset >= offset)) {
        return okStatus();
    }
    entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(keep));
    entries_.front().startOffset = offset;
    return persist();
}

Status LeaderEpochCache::persist() const {
    std::ostringstream out;
    out << kVersion << '\n' << entries_.size() << '\n';
    for (const EpochEntry& e : entries_) {
        out << e.epoch << ' ' << e.startOffset << '\n';
    }
    return writeFileAtomically(path_, out.str());
}

}  // namespace elog
