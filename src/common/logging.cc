#include "elog/common/logging.h"

#include <mutex>
#include <vector>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_sinks.h>

namespace elog::logging {

namespace {

// Shared by every component logger; set by init().
struct Settings {
    std::mutex mu;
    std::vector<spdlog::sink_ptr> sinks{std::make_shared<spdlog::sinks::stderr_sink_mt>()};
    std::string pattern = "%Y-%m-%dT%H:%M:%S.%e %l %n - %v";
    spdlog::level::level_enum level = spdlog::level::info;
};

Settings& settings() {
    static Settings instance;
    return instance;
}

}  // namespace

Status init(const std::string& node, const std::optional<std::string>& file,
            spdlog::level::level_enum level) {
    std::vector<spdlog::sink_ptr> sinks{std::make_shared<spdlog::sinks::stderr_sink_mt>()};
    if (file) {
        try {
            sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_mt>(*file));
        } catch (const spdlog::spdlog_ex& e) {
            return {ErrorCode::INVALID_REQUEST, "cannot open log file " + *file + ": " + e.what()};
        }
    }

    auto& s = settings();
    std::lock_guard lock(s.mu);
    s.sinks = std::move(sinks);
    s.pattern = "%Y-%m-%dT%H:%M:%S.%e %l %n " + node + " %v";
    s.level = level;
    spdlog::drop_all();  // loggers made before init() would keep the old sinks
    return okStatus();
}

std::shared_ptr<spdlog::logger> get(const std::string& component) {
    auto& s = settings();
    std::lock_guard lock(s.mu);
    if (auto existing = spdlog::get(component)) {
        return existing;
    }
    auto logger = std::make_shared<spdlog::logger>(component, s.sinks.begin(), s.sinks.end());
    logger->set_pattern(s.pattern);
    logger->set_level(s.level);
    // Flush every line: a process killed with SIGKILL by a fault test must not lose the
    // lines that explain what it was doing.
    logger->flush_on(spdlog::level::trace);
    spdlog::register_logger(logger);
    return logger;
}

}  // namespace elog::logging
