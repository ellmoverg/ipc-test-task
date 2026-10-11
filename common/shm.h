#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace ipc {

inline constexpr const char* kShmName = "/ipc-test-ring";

class SharedMemory {
public:
    static SharedMemory create(std::string name, std::size_t size) {
        shm_unlink(name.c_str()); // ENOENT is the normal case
        const int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
        if (fd < 0)
            fail("shm_open");
        SharedMemory shm;
        shm.owned_name_ = std::move(name);
        if (ftruncate(fd, static_cast<off_t>(size)) != 0)
            fail("ftruncate", fd);
        shm.map(fd, size);
        return shm;
    }

    static std::optional<SharedMemory> try_open(const char* name) {
        const int fd = shm_open(name, O_RDWR, 0);
        if (fd < 0) {
            if (errno == ENOENT)
                return std::nullopt;
            fail("shm_open");
        }
        struct stat st;
        if (fstat(fd, &st) != 0)
            fail("fstat", fd);
        if (st.st_size == 0) {
            close(fd);
            return std::nullopt;
        }
        SharedMemory shm;
        shm.map(fd, static_cast<std::size_t>(st.st_size));
        return shm;
    }

    SharedMemory(SharedMemory&& other) noexcept
        : data_(std::exchange(other.data_, nullptr)),
          size_(other.size_),
          owned_name_(std::exchange(other.owned_name_, {})) {}
    SharedMemory& operator=(SharedMemory&&) = delete;

    ~SharedMemory() {
        if (data_)
            munmap(data_, size_);
        if (!owned_name_.empty())
            shm_unlink(owned_name_.c_str());
    }

    void* data() const { return data_; }
    std::size_t size() const { return size_; }

private:
    SharedMemory() = default;

    void map(int fd, std::size_t size) {
        void* p = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (p == MAP_FAILED)
            fail("mmap", fd);
        close(fd);
        data_ = p;
        size_ = size;
    }

    [[noreturn]] static void fail(const char* what, int fd = -1) {
        const int err = errno;
        if (fd >= 0)
            close(fd);
        throw std::system_error(err, std::generic_category(), what);
    }

    void* data_ = nullptr;
    std::size_t size_ = 0;
    std::string owned_name_;
};

}
