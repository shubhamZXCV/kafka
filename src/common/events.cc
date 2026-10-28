#include "elog/common/events.h"

#include <filesystem>
#include <format>

#include "elog/common/time.h"

namespace elog {

namespace {

// RFC 4180 quoting, so a detail or member ID containing a comma stays one field.
std::string csvField(const std::string& s) {
    if (s.find_first_of(",\"\n") == std::string::npos) {
        return s;
    }
    std::string quoted = "\"";
    for (const char c : s) {
        quoted += c;
        if (c == '"') {
            quoted += '"';
        }
    }
    return quoted + "\"";
}

template <class T>
std::string optionalField(const std::optional<T>& v) {
    return v ? std::to_string(*v) : std::string();
}

}  // namespace

Result<std::unique_ptr<EventLog>> EventLog::open(const std::string& dir, const std::string& node) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    const std::string path = dir + "/events-" + node + ".csv";
    // A restarted node appends to its old file; write the header only once.
    std::error_code sizeEc;
    const bool isNew = std::filesystem::file_size(path, sizeEc) == 0 || sizeEc;  // empty or missing
    std::ofstream out(path, std::ios::app);
    if (ec || !out) {
        return {ErrorCode::INVALID_REQUEST, "cannot open events file " + path};
    }
    if (isNew) {
        out << "ts_ms,node,event,group,topic,partition,generation,epoch,member,offset,detail\n";
    }
    out.flush();
    return std::unique_ptr<EventLog>(new EventLog(std::move(out), node));
}

void EventLog::emit(const Event& e) {
    const std::string line = std::format(
        "{},{},{},{},{},{},{},{},{},{},{}\n", time::wallMs(), node_, e.event, csvField(e.group),
        e.tp ? csvField(e.tp->topic) : "", e.tp ? std::to_string(e.tp->partition) : "",
        optionalField(e.generation), optionalField(e.epoch), csvField(e.member),
        optionalField(e.offset), csvField(e.detail));
    std::lock_guard lock(mu_);
    out_ << line;
    out_.flush();
}

}  // namespace elog
