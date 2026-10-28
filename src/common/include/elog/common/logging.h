#pragma once

#include <memory>
#include <optional>
#include <string>

#include <spdlog/spdlog.h>

#include "elog/common/result.h"

// Structured logging over spdlog (LLD §13.1). Every line has the form
//
//     <ts> <level> <component> <node> <event> key=value key=value ...
//
// e.g. "2026-10-29T10:15:02.123 info storage broker-1 segment_rolled tp=orders-3 base=5000".
// Log one line per significant event and never inside per-record loops
// (implementation plan §4).
//
//     auto log = logging::get("storage");
//     log->info("segment_rolled tp={} base={}", tp, baseOffset);
//
// Thread-safe: loggers may be fetched and used from any thread.
namespace elog::logging {

// Call once at startup, before any get(). Lines go to stderr and, if `file` is set, are
// appended to that file. `node` names this process, e.g. "broker-1" or "coordinator".
Status init(const std::string& node, const std::optional<std::string>& file,
            spdlog::level::level_enum level = spdlog::level::info);

// The logger for one component ("storage", "replication", "group", ...). Created on first
// use and shared afterwards.
std::shared_ptr<spdlog::logger> get(const std::string& component);

}  // namespace elog::logging
