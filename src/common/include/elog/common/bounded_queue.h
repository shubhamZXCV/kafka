#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>

namespace elog {

// A fixed-capacity FIFO queue shared by producer and consumer threads (LLD §3.4). This is the
// bounded buffer behind all backpressure: the coordinator inbound queue, the producer
// accumulator and the consumer in-flight queue. A full queue makes push() wait, which slows
// the producer down instead of letting memory grow (FR-BP-*).
//
// Thread-safe: any number of threads may push and pop concurrently.
template <class T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

    // Waits up to `timeout` for a free slot. Returns false on timeout or if the queue is
    // closed; the item is then dropped.
    bool push(T item, std::chrono::milliseconds timeout) {
        std::unique_lock lock(mu_);
        const bool ready =
            notFull_.wait_for(lock, timeout, [&] { return closed_ || items_.size() < capacity_; });
        if (!ready || closed_) {
            return false;
        }
        items_.push_back(std::move(item));
        notEmpty_.notify_one();
        return true;
    }

    // Waits up to `timeout` for an item. Returns nullopt on timeout, or once the queue is
    // closed and empty. Items queued before close() are still handed out.
    std::optional<T> pop(std::chrono::milliseconds timeout) {
        std::unique_lock lock(mu_);
        const bool ready =
            notEmpty_.wait_for(lock, timeout, [&] { return closed_ || !items_.empty(); });
        if (!ready || items_.empty()) {
            return std::nullopt;
        }
        T item = std::move(items_.front());
        items_.pop_front();
        notFull_.notify_one();
        return item;
    }

    std::size_t size() const {
        std::lock_guard lock(mu_);
        return items_.size();
    }

    // capacity - size: the consumer's fetch credit (FR-FETCH-04).
    std::size_t freeSlots() const {
        std::lock_guard lock(mu_);
        return capacity_ - items_.size();
    }

    // Removes every item matching `pred` and returns how many were removed. Used on partition
    // revocation to drop records of partitions this consumer no longer owns.
    template <class Pred>
    std::size_t removeIf(Pred pred) {
        std::lock_guard lock(mu_);
        const auto removed = std::erase_if(items_, pred);
        if (removed > 0) {
            notFull_.notify_all();
        }
        return removed;
    }

    // Wakes every waiting thread. Later pushes fail; pops drain what is left.
    void close() {
        std::lock_guard lock(mu_);
        closed_ = true;
        notFull_.notify_all();
        notEmpty_.notify_all();
    }

private:
    mutable std::mutex mu_;
    std::condition_variable notFull_;   // signalled when a slot frees up
    std::condition_variable notEmpty_;  // signalled when an item arrives
    std::deque<T> items_;
    const std::size_t capacity_;
    bool closed_ = false;
};

}  // namespace elog
