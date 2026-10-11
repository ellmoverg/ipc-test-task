#include "controls.h"
#include "layout.h"
#include "ring.h"
#include "shm.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <new>
#include <string_view>
#include <thread>

namespace {

constexpr std::size_t kRingBytes = 4 << 20;
constexpr auto kRetryDelay = std::chrono::microseconds(50);

std::uint32_t parse_payload_size(std::string_view arg) {
    std::uint32_t size = 0;
    const auto [end, ec] = std::from_chars(arg.data(), arg.data() + arg.size(), size);
    if (ec != std::errc{} || end != arg.data() + arg.size())
        return 0;
    return size;
}

}

int main(int argc, char* argv[]) {
    const std::uint32_t payload_size = argc == 2 ? parse_payload_size(argv[1]) : 0;
    if (payload_size == 0) {
        std::fprintf(stderr, "usage: %s <payload size in bytes>\n", argv[0]);
        return EXIT_FAILURE;
    }

    try {
        ipc::Controls controls;
        const std::size_t stride = ipc::slot_stride(payload_size);
        const auto slot_count = static_cast<std::uint32_t>(
            std::bit_floor(std::max<std::size_t>(kRingBytes / stride, 2)));
        const std::size_t size = ipc::segment_size(slot_count, payload_size);

        auto shm = ipc::SharedMemory::create(ipc::kShmName, size);
        auto* cb = new (shm.data()) ipc::ControlBlock{};
        cb->slot_count = slot_count;
        cb->slot_size = stride;
        cb->payload_size = payload_size;
        cb->state.store(ipc::State::Ready, std::memory_order_release);
        std::printf("producer: payload %" PRIu32 " B, %" PRIu32 " slots of %zu B, segment %zu B\n",
                    payload_size, slot_count, stride, size);

        ipc::RingWriter writer(cb);
        std::uint64_t seq = 0;
        while (!ipc::g_stop) {
            std::byte* slot;
            while (!(slot = writer.try_reserve()) && !ipc::g_stop)
                std::this_thread::sleep_for(kRetryDelay);
            if (!slot)
                break;

            auto* hdr = reinterpret_cast<ipc::PacketHeader*>(slot);
            std::byte* payload = slot + sizeof(ipc::PacketHeader);
            hdr->seq = seq;
            hdr->timestamp_ns = ipc::now_ns();
            hdr->payload_size = payload_size;
            std::memset(payload, static_cast<int>(seq & 0xFF), payload_size);
            hdr->checksum = ipc::packet_checksum(*hdr, payload);
            writer.commit();
            ++seq;
        }

        cb->state.store(ipc::State::Closed, std::memory_order_release);
        std::printf("producer: stopped after %" PRIu64 " packets\n", seq);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "producer: %s\n", e.what());
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
