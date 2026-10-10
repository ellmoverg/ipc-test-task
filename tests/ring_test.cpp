#include "ring.h"

#include <gtest/gtest.h>

#include <limits>
#include <new>
#include <optional>

namespace {

constexpr std::uint32_t kSlots = 4;
constexpr std::size_t kPayload = 200;

class RingTest : public ::testing::Test {
protected:
    RingTest() {
        mem_ = ::operator new(ipc::segment_size(kSlots, kPayload), std::align_val_t{ipc::kCacheLine});
        cb_ = new (mem_) ipc::ControlBlock{};
        cb_->slot_count = kSlots;
        cb_->slot_size = ipc::slot_stride(kPayload);
        cb_->payload_size = kPayload;
    }

    ~RingTest() override { ::operator delete(mem_, std::align_val_t{ipc::kCacheLine}); }

    std::byte* slot_at(std::uint64_t counter) const {
        return ipc::slots_base(cb_) + (counter % kSlots) * ipc::slot_stride(kPayload);
    }

    void* mem_;
    ipc::ControlBlock* cb_;
};

bool push(ipc::RingWriter& writer, std::uint64_t seq) {
    std::byte* slot = writer.try_reserve();
    if (!slot)
        return false;
    reinterpret_cast<ipc::PacketHeader*>(slot)->seq = seq;
    writer.commit();
    return true;
}

std::optional<std::uint64_t> pop(ipc::RingReader& reader) {
    const std::byte* slot = reader.try_peek();
    if (!slot)
        return std::nullopt;
    const std::uint64_t seq = reinterpret_cast<const ipc::PacketHeader*>(slot)->seq;
    reader.release();
    return seq;
}

TEST_F(RingTest, EmptyRingHasNothingToRead) {
    ipc::RingReader reader(cb_);
    EXPECT_EQ(reader.try_peek(), nullptr);
}

TEST_F(RingTest, HoldsExactlyCapacity) {
    ipc::RingWriter writer(cb_);
    for (std::uint64_t i = 0; i < kSlots; ++i)
        ASSERT_TRUE(push(writer, i)) << "packet " << i;
    EXPECT_EQ(writer.try_reserve(), nullptr);
}

TEST_F(RingTest, ReleaseFreesOneSlot) {
    ipc::RingWriter writer(cb_);
    ipc::RingReader reader(cb_);
    for (std::uint64_t i = 0; i < kSlots; ++i)
        ASSERT_TRUE(push(writer, i));
    ASSERT_EQ(writer.try_reserve(), nullptr);

    ASSERT_EQ(pop(reader), 0u);
    EXPECT_TRUE(push(writer, kSlots));
    EXPECT_EQ(writer.try_reserve(), nullptr);
}

TEST_F(RingTest, SlotsAreStrideApartAndWrap) {
    ipc::RingWriter writer(cb_);
    ipc::RingReader reader(cb_);
    for (std::uint64_t i = 0; i < 3 * kSlots; ++i) {
        std::byte* written = writer.try_reserve();
        ASSERT_EQ(written, slot_at(i)) << "packet " << i;
        writer.commit();
        EXPECT_EQ(reader.try_peek(), written) << "packet " << i;
        reader.release();
    }
}

TEST_F(RingTest, FifoAcrossManyWraps) {
    ipc::RingWriter writer(cb_);
    ipc::RingReader reader(cb_);
    std::uint64_t next_in = 0;
    std::uint64_t next_out = 0;
    for (std::uint32_t round = 0; round < 10; ++round) {
        while (push(writer, next_in))
            ++next_in;
        for (std::uint32_t k = 0; k <= round % kSlots; ++k)
            ASSERT_EQ(pop(reader), next_out++);
    }
    while (auto seq = pop(reader))
        EXPECT_EQ(*seq, next_out++);
    EXPECT_EQ(next_out, next_in);
}

TEST_F(RingTest, PublishesCountersOnlyOnCommitAndRelease) {
    ipc::RingWriter writer(cb_);
    ipc::RingReader reader(cb_);
    for (std::uint64_t i = 0; i < 3; ++i)
        ASSERT_TRUE(push(writer, i));
    ASSERT_NE(writer.try_reserve(), nullptr);
    EXPECT_EQ(cb_->head.load(), 3u);
    EXPECT_EQ(cb_->tail.load(), 0u);

    ASSERT_TRUE(pop(reader));
    ASSERT_NE(reader.try_peek(), nullptr);
    EXPECT_EQ(cb_->head.load(), 3u);
    EXPECT_EQ(cb_->tail.load(), 1u);
}

TEST_F(RingTest, SurvivesCounterOverflow) {
    constexpr std::uint64_t start = std::numeric_limits<std::uint64_t>::max() - 1;
    cb_->head.store(start);
    cb_->tail.store(start);
    ipc::RingWriter writer(cb_);
    ipc::RingReader reader(cb_);

    for (std::uint64_t i = 0; i < kSlots; ++i) {
        ASSERT_EQ(writer.try_reserve(), slot_at(start + i)) << "packet " << i;
        ASSERT_TRUE(push(writer, i));
    }
    EXPECT_EQ(writer.try_reserve(), nullptr);
    EXPECT_EQ(cb_->head.load(), 2u);

    for (std::uint64_t i = 0; i < kSlots; ++i)
        EXPECT_EQ(pop(reader), i);
    EXPECT_EQ(reader.try_peek(), nullptr);
}

}
