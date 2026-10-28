#pragma once

#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "elog/common/result.h"
#include "elog/common/types.h"

namespace elog {

// One row of events-<node>.csv (LLD §13.1, catalogue in §13.3). Fill only the fields the
// event has:
//
//     events.emit({.event = "commit_fenced", .group = g, .tp = tp, .epoch = e, .member = m});
struct Event {
    std::string event;
    std::string group = {};
    std::optional<TopicPartition> tp = {};
    std::optional<Generation> generation = {};
    std::optional<Epoch> epoch = {};
    std::string member = {};
    std::optional<Offset> offset = {};
    std::string detail = {};  // anything else, as key=value pairs separated by ';'
};

// Appends events to <dir>/events-<node>.csv with columns
//
//     ts_ms,node,event,group,topic,partition,generation,epoch,member,offset,detail
//
// Each event is flushed at once: events are rare, and the analysis scripts and functional
// tests must see them even if the process is killed right after.
//
// Thread-safe.
class EventLog {
public:
    static Result<std::unique_ptr<EventLog>> open(const std::string& dir, const std::string& node);

    void emit(const Event& e);

private:
    EventLog(std::ofstream out, std::string node) : node_(std::move(node)), out_(std::move(out)) {}

    const std::string node_;
    std::mutex mu_;  // guards out_
    std::ofstream out_;
};

}  // namespace elog
