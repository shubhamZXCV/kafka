#include "elog/storage/log_verifier.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iterator>

#include "elog/protocol/record_batch.h"

namespace elog {

namespace {

namespace fs = std::filesystem;

// Checks one segment file. Its first batch must start where the previous segment ended
// (report.nextOffset); the log's first segment sets where the log starts.
void verifySegment(const fs::path& file, Offset base, bool first, PartitionReport& report) {
    std::ifstream in(file, std::ios::binary);
    const std::vector<std::byte> bytes = [&] {
        std::vector<char> raw((std::istreambuf_iterator<char>(in)), {});
        std::vector<std::byte> out(raw.size());
        std::transform(raw.begin(), raw.end(), out.begin(), [](char c) { return std::byte(c); });
        return out;
    }();
    if (first) {
        report.firstOffset = report.nextOffset = base;
    } else if (base != report.nextOffset) {
        report.errors.push_back(file.filename().string() + ": starts at " + std::to_string(base) +
                                ", previous segment ended at " + std::to_string(report.nextOffset));
    }
    std::size_t pos = 0;
    while (pos < bytes.size()) {
        const auto rest = std::span(bytes).subspan(pos);
        if (rest.size() < batch::kHeaderBytes) {
            report.errors.push_back(file.filename().string() + ": " + std::to_string(rest.size()) +
                                    " trailing bytes at position " + std::to_string(pos));
            return;
        }
        const std::size_t total = batch::kRecordBatchOverhead + readBatchHeader(rest).batchLength;
        auto view = RecordBatchView::parse(rest.first(std::min(total, rest.size())));
        if (!view.ok()) {
            report.errors.push_back(file.filename().string() + ": position " + std::to_string(pos) +
                                    ": " + view.message());
            return;
        }
        if (view.value().baseOffset() != report.nextOffset) {
            report.errors.push_back(file.filename().string() + ": batch at " +
                                    std::to_string(view.value().baseOffset()) + ", expected " +
                                    std::to_string(report.nextOffset));
        }
        report.nextOffset = view.value().lastOffset() + 1;
        ++report.batches;
        pos += total;
    }
}

}  // namespace

std::vector<PartitionReport> verifyDataDir(const std::string& dataDir) {
    std::vector<PartitionReport> reports;
    std::vector<fs::path> partitions;
    for (const auto& entry : fs::directory_iterator(dataDir)) {
        if (entry.is_directory()) {
            partitions.push_back(entry.path());
        }
    }
    std::sort(partitions.begin(), partitions.end());
    for (const fs::path& dir : partitions) {
        PartitionReport report;
        report.name = dir.filename().string();
        std::vector<std::pair<Offset, fs::path>> segments;
        for (const auto& entry : fs::directory_iterator(dir)) {
            const auto name = entry.path().filename().string();
            if (entry.path().extension() == ".log" && name.size() == 24) {
                segments.emplace_back(std::stoull(name.substr(0, 20)), entry.path());
            }
        }
        std::sort(segments.begin(), segments.end());
        report.segments = segments.size();
        for (std::size_t i = 0; i < segments.size(); ++i) {
            verifySegment(segments[i].second, segments[i].first, i == 0, report);
        }
        reports.push_back(std::move(report));
    }
    return reports;
}

}  // namespace elog
