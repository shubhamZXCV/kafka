#pragma once

#include <chrono>
#include <thread>

#include "elog/common/bounded_queue.h"
#include "elog/coordinator/event.h"

namespace elog {

// Sends LeaderAndIsr to brokers on its own thread, so the event loop never waits on the
// network (LLD §7.2). It never touches coordinator state: it gets complete requests and
// reports failures back as PushResultEvent on the loop's inbound queue.
//
// Thread rules: enqueue() is called by the event loop; start()/stop() by the server
// thread. Everything else runs on the pusher thread.
class BrokerPusher {
public:
    explicit BrokerPusher(BoundedQueue<LoopEvent>& results);

    void start();
    void enqueue(LeaderAndIsrPush push);
    void stop();  // drops pending pushes and joins the thread

private:
    void run();
    ErrorCode deliver(const LeaderAndIsrPush& push);

    BoundedQueue<LoopEvent>& results_;
    BoundedQueue<LeaderAndIsrPush> pending_;
    std::thread thread_;
    std::uint32_t nextCorrelationId_ = 1;  // pusher thread only
};

}  // namespace elog
