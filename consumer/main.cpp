#include "layout.h"
#include "ring.h"
#include "shm.h"

#include <atomic>
#include <bit>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <optional>
#include <stdexcept>
#include <thread>

namespace {

constexpr auto kRetryDelay = std::chrono::microseconds(50);
constexpr auto kAttachDelay = std::chrono::milliseconds(100);

std::atomic<bool> g_stop{false};

void on_signal(int) { g_stop = true; }

struct Counters {
    std::atomic<std::uint64_t> packets{0};
    std::atomic<std::uint64_t> bytes{0};
    std::atomic<std::uint64_t> corrupt{0};
    std::atomic<std::uint64_t> out_of_order{0};
    std::atomic<std::uint64_t> time_reversed{0};
};

void add(std::atomic<std::uint64_t>& counter, std::uint64_t n) {
    counter.store(counter.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
}

std::uint64_t get(const std::atomic<std::uint64_t>& counter) {
    return counter.load(std::memory_order_relaxed);
}

void report_every_second(const Counters& c) {
    using clock = std::chrono::steady_clock;
    auto last_time = clock::now();
    auto next = last_time;
    std::uint64_t last_packets = 0;
    std::uint64_t last_bytes = 0;
    for (;;) {
        next += std::chrono::seconds(1);
        std::this_thread::sleep_until(next);
        if (g_stop)
            return;
        const auto now = clock::now();
        const double seconds = std::chrono::duration<double>(now - last_time).count();
        const std::uint64_t packets = get(c.packets);
        const std::uint64_t bytes = get(c.bytes);
        std::printf("packets %" PRIu64 " | %.0f pkt/s | %.1f MB/s | corrupt %" PRIu64
                    ", out of order %" PRIu64 ", time reversed %" PRIu64 "\n",
                    packets, static_cast<double>(packets - last_packets) / seconds,
                    static_cast<double>(bytes - last_bytes) / seconds / 1e6, get(c.corrupt),
                    get(c.out_of_order), get(c.time_reversed));
        std::fflush(stdout);
        last_time = now;
        last_packets = packets;
        last_bytes = bytes;
    }
}

std::optional<ipc::SharedMemory> attach() {
    while (!g_stop) {
        auto shm = ipc::SharedMemory::try_open(ipc::kShmName);
        if (shm && shm->size() >= sizeof(ipc::ControlBlock)) {
            const auto* cb = static_cast<const ipc::ControlBlock*>(shm->data());
            if (cb->state.load(std::memory_order_acquire) == ipc::State::Ready)
                return shm;
        }
        std::this_thread::sleep_for(kAttachDelay);
    }
    return std::nullopt;
}

void check_geometry(const ipc::ControlBlock& cb, std::size_t mapped_size) {
    if (!std::has_single_bit(cb.slot_count) || cb.payload_size == 0 ||
        cb.slot_size != ipc::slot_stride(cb.payload_size) ||
        ipc::segment_size(cb.slot_count, cb.payload_size) > mapped_size)
        throw std::runtime_error("shared memory geometry doesn't match its size");
}

}

int main() {
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    try {
        std::puts("consumer: waiting for producer");
        std::optional<ipc::SharedMemory> shm = attach();
        if (!shm)
            return EXIT_SUCCESS;
        auto* cb = static_cast<ipc::ControlBlock*>(shm->data());
        check_geometry(*cb, shm->size());
        const std::uint64_t payload_size = cb->payload_size;
        std::printf("consumer: attached, payload %" PRIu64 " B, %" PRIu32 " slots\n", payload_size,
                    cb->slot_count);

        ipc::RingReader reader(cb);
        Counters counters;
        std::thread reporter(report_every_second, std::cref(counters));
        std::optional<std::uint64_t> expected_seq;
        std::uint64_t last_timestamp = 0;

        while (!g_stop) {
            const std::byte* slot = reader.try_peek();
            if (!slot) {
                if (cb->state.load(std::memory_order_acquire) == ipc::State::Closed &&
                    !reader.try_peek())
                    break;
                std::this_thread::sleep_for(kRetryDelay);
                continue;
            }

            const auto* hdr = reinterpret_cast<const ipc::PacketHeader*>(slot);
            const std::byte* payload = slot + sizeof(ipc::PacketHeader);
            if (hdr->payload_size != payload_size || ipc::packet_checksum(*hdr, payload) != hdr->checksum)
                add(counters.corrupt, 1);
            if (expected_seq && hdr->seq != *expected_seq)
                add(counters.out_of_order, 1);
            if (hdr->timestamp_ns < last_timestamp)
                add(counters.time_reversed, 1);
            expected_seq = hdr->seq + 1;
            last_timestamp = hdr->timestamp_ns;
            add(counters.packets, 1);
            add(counters.bytes, payload_size);
            reader.release();
        }

        g_stop = true;
        reporter.join();
        std::printf("consumer: stopped after %" PRIu64 " packets, corrupt %" PRIu64
                    ", out of order %" PRIu64 ", time reversed %" PRIu64 "\n",
                    get(counters.packets), get(counters.corrupt), get(counters.out_of_order),
                    get(counters.time_reversed));
    } catch (const std::exception& e) {
        std::fprintf(stderr, "consumer: %s\n", e.what());
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
