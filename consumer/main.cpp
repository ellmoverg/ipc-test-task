#include "controls.h"
#include "layout.h"
#include "ring.h"
#include "shm.h"

#include <atomic>
#include <bit>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <optional>
#include <stdexcept>
#include <thread>

namespace {

constexpr auto kRetryDelay = std::chrono::microseconds(50);
constexpr auto kAttachDelay = std::chrono::milliseconds(100);

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

// Runs on the control thread once a second.
class Reporter {
public:
    explicit Reporter(const Counters& c) : c_(c) {}

    void tick() {
        const auto now = std::chrono::steady_clock::now();
        const double seconds = std::chrono::duration<double>(now - last_time_).count();
        const std::uint64_t packets = get(c_.packets);
        const std::uint64_t bytes = get(c_.bytes);
        std::printf("packets %" PRIu64 " | %.0f pkt/s | %.1f MB/s | corrupt %" PRIu64
                    ", out of order %" PRIu64 ", time reversed %" PRIu64 "%s\n",
                    packets, static_cast<double>(packets - last_packets_) / seconds,
                    static_cast<double>(bytes - last_bytes_) / seconds / 1e6, get(c_.corrupt),
                    get(c_.out_of_order), get(c_.time_reversed), ipc::g_paused ? " [paused]" : "");
        std::fflush(stdout);
        last_time_ = now;
        last_packets_ = packets;
        last_bytes_ = bytes;
    }

private:
    const Counters& c_;
    std::chrono::steady_clock::time_point last_time_ = std::chrono::steady_clock::now();
    std::uint64_t last_packets_ = 0;
    std::uint64_t last_bytes_ = 0;
};

ipc::SharedMemory attach() {
    for (;;) {
        auto shm = ipc::SharedMemory::try_open(ipc::kShmName);
        if (shm && shm->size() >= sizeof(ipc::ControlBlock)) {
            const auto* cb = static_cast<const ipc::ControlBlock*>(shm->data());
            if (cb->state.load(std::memory_order_acquire) == ipc::State::Ready)
                return std::move(*shm);
        }
        std::this_thread::sleep_for(kAttachDelay);
    }
}

void check_geometry(const ipc::ControlBlock& cb, std::size_t mapped_size) {
    if (!std::has_single_bit(cb.slot_count) || cb.payload_size == 0 ||
        cb.slot_size != ipc::slot_stride(cb.payload_size) ||
        ipc::segment_size(cb.slot_count, cb.payload_size) > mapped_size)
        throw std::runtime_error("shared memory geometry doesn't match its size");
}

}

int main() {
    try {
        std::puts("consumer: waiting for producer");
        ipc::SharedMemory shm = attach();
        auto* cb = static_cast<ipc::ControlBlock*>(shm.data());
        check_geometry(*cb, shm.size());
        const std::uint64_t payload_size = cb->payload_size;
        std::printf("consumer: attached, payload %" PRIu64 " B, %" PRIu32 " slots\n", payload_size,
                    cb->slot_count);

        ipc::RingReader reader(cb);
        Counters counters;
        Reporter reporter(counters);
        ipc::Controls controls("consumer", [&reporter] { reporter.tick(); });
        std::optional<std::uint64_t> expected_seq;
        std::uint64_t last_timestamp = 0;

        while (!ipc::g_stop) {
            if (ipc::g_paused) {
                std::this_thread::sleep_for(ipc::kPauseDelay);
                continue;
            }
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

        controls.stop();
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
