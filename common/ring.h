#pragma once

#include "layout.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ipc {

class RingWriter {
public:
    explicit RingWriter(ControlBlock* cb)
        : cb_(cb),
          slots_(slots_base(cb)),
          capacity_(cb->slot_count),
          mask_(cb->slot_count - 1),
          stride_(cb->slot_size),
          head_(cb->head.load(std::memory_order_relaxed)),
          cached_tail_(cb->tail.load(std::memory_order_acquire)) {}

    std::byte* try_reserve() noexcept {
        if (head_ - cached_tail_ == capacity_) {
            cached_tail_ = cb_->tail.load(std::memory_order_acquire);
            if (head_ - cached_tail_ == capacity_)
                return nullptr;
        }
        return slots_ + (head_ & mask_) * stride_;
    }

    void commit() noexcept { cb_->head.store(++head_, std::memory_order_release); }

private:
    ControlBlock* cb_;
    std::byte* slots_;
    std::uint64_t capacity_;
    std::uint64_t mask_;
    std::size_t stride_;
    std::uint64_t head_;
    std::uint64_t cached_tail_;
};

class RingReader {
public:
    explicit RingReader(ControlBlock* cb)
        : cb_(cb),
          slots_(slots_base(cb)),
          mask_(cb->slot_count - 1),
          stride_(cb->slot_size),
          tail_(cb->tail.load(std::memory_order_relaxed)),
          cached_head_(cb->head.load(std::memory_order_acquire)) {}

    const std::byte* try_peek() noexcept {
        if (tail_ == cached_head_) {
            cached_head_ = cb_->head.load(std::memory_order_acquire);
            if (tail_ == cached_head_)
                return nullptr;
        }
        return slots_ + (tail_ & mask_) * stride_;
    }

    void release() noexcept { cb_->tail.store(++tail_, std::memory_order_release); }

private:
    ControlBlock* cb_;
    const std::byte* slots_;
    std::uint64_t mask_;
    std::size_t stride_;
    std::uint64_t tail_;
    std::uint64_t cached_head_;
};

}
