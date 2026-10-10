#pragma once


#include <zlib.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ipc {

// не використовуємо hardware_destructive_interference_size
inline constexpr std::size_t kCacheLine = 128;

constexpr std::size_t align_up(std::size_t n, std::size_t a) {
    return (n + a - 1) / a * a;
}

struct PacketHeader {
    std::uint64_t seq;
    std::uint64_t timestamp_ns;
    std::uint32_t payload_size;
    std::uint32_t checksum;
};

inline std::uint32_t packet_checksum(const PacketHeader& h, const std::byte* payload) {
    auto feed = [](uLong crc, const void* p, std::size_t n) {
        return crc32(crc, static_cast<const Bytef*>(p), static_cast<uInt>(n));
    };
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = feed(crc, &h.seq, sizeof h.seq);
    crc = feed(crc, &h.timestamp_ns, sizeof h.timestamp_ns);
    crc = feed(crc, &h.payload_size, sizeof h.payload_size);
    crc = feed(crc, payload, h.payload_size);
    return static_cast<std::uint32_t>(crc);
}

enum class State : std::uint32_t {
    Initializing = 0,
    Ready = 1,
    Closed = 2,
};

struct alignas(kCacheLine) ControlBlock {
    std::uint32_t slot_count;
    std::uint64_t slot_size;
    std::uint64_t payload_size;
    std::atomic<State> state;

    alignas(kCacheLine) std::atomic<std::uint64_t> head;
    alignas(kCacheLine) std::atomic<std::uint64_t> tail;
};

static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic<State>::is_always_lock_free);

constexpr std::size_t slot_stride(std::size_t payload_size) {
    return align_up(sizeof(PacketHeader) + payload_size, kCacheLine);
}

constexpr std::size_t segment_size(std::uint32_t slot_count, std::size_t payload_size) {
    return sizeof(ControlBlock) + slot_count * slot_stride(payload_size);
}

inline std::byte* slots_base(ControlBlock* cb) {
    return reinterpret_cast<std::byte*>(cb) + sizeof(ControlBlock);
}

}
