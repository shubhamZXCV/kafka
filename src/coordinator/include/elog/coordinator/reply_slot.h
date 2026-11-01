#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <utility>

#include "elog/protocol/frame.h"

namespace elog {

// Where the event loop leaves the response to one request; the connection thread that
// queued the request waits on it (LLD §7.1–7.2).
//
// Thread rules: complete() is called once, by the event loop; wait() by one connection
// thread.
class ReplySlot {
public:
    void complete(Frame response) {
        {
            std::lock_guard lock(mu_);  // ReplySlot::mu_ guards only resp_ (LLD §11.1)
            resp_ = std::move(response);
        }
        cv_.notify_all();
    }

    Frame wait() {
        std::unique_lock lock(mu_);
        cv_.wait(lock, [&] { return resp_.has_value(); });
        return std::move(*resp_);
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::optional<Frame> resp_;
};

}  // namespace elog
