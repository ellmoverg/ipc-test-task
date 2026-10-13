#pragma once

#include <fcntl.h>
#include <poll.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <system_error>
#include <thread>
#include <utility>

namespace ipc {

inline std::atomic<bool> g_stop{false};
inline std::atomic<bool> g_paused{false};

inline constexpr auto kPauseDelay = std::chrono::milliseconds(10);

inline int g_signal_pipe = -1;

inline void forward_signal(int signo) {
    const int saved_errno = errno;
    const auto byte = static_cast<unsigned char>(signo);
    [[maybe_unused]] auto n = write(g_signal_pipe, &byte, 1);
    errno = saved_errno;
}

class Controls {
public:
    Controls(const char* name, std::function<void()> on_tick = {})
        : name_(name), on_tick_(std::move(on_tick)) {
        if (pipe(pipe_) != 0)
            throw std::system_error(errno, std::generic_category(), "pipe");
        for (int fd : pipe_)
            fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
        g_signal_pipe = pipe_[1];
        for (int signo : {SIGINT, SIGTERM, SIGUSR1})
            std::signal(signo, forward_signal);

        terminal_saved_ = tcgetattr(STDIN_FILENO, &saved_terminal_) == 0;
        if (terminal_saved_) {
            termios raw = saved_terminal_;
            raw.c_lflag &= ~static_cast<tcflag_t>(ICANON | ECHO);
            tcsetattr(STDIN_FILENO, TCSANOW, &raw);
        }

        thread_ = std::thread(&Controls::run, this);
    }

    Controls(const Controls&) = delete;
    Controls& operator=(const Controls&) = delete;

    ~Controls() {
        stop();
        if (terminal_saved_)
            tcsetattr(STDIN_FILENO, TCSANOW, &saved_terminal_);
        for (int signo : {SIGINT, SIGTERM, SIGUSR1})
            std::signal(signo, SIG_DFL);
        close(pipe_[0]);
        close(pipe_[1]);
    }

    void stop() {
        g_stop = true;
        forward_signal(0);
        if (thread_.joinable())
            thread_.join();
    }

private:
    void run() {
        using clock = std::chrono::steady_clock;
        auto next_tick = clock::now() + std::chrono::seconds(1);
        pollfd fds[] = {{pipe_[0], POLLIN, 0}, {STDIN_FILENO, POLLIN, 0}};

        while (!g_stop) {
            int timeout_ms = -1;
            if (on_tick_) {
                const auto left = std::chrono::ceil<std::chrono::milliseconds>(next_tick - clock::now());
                timeout_ms = static_cast<int>(std::max<std::int64_t>(left.count(), 0));
            }
            poll(fds, 2, timeout_ms);

            if (fds[0].revents & POLLIN)
                handle_signals();
            if (fds[1].revents)
                handle_keys(fds[1]);
            if (on_tick_ && clock::now() >= next_tick) {
                on_tick_();
                next_tick += std::chrono::seconds(1);
            }
        }
    }

    void handle_signals() {
        unsigned char signo;
        while (read(pipe_[0], &signo, 1) == 1) {
            if (signo == SIGINT || signo == SIGTERM)
                g_stop = true;
            else if (signo == SIGUSR1)
                toggle_pause();
        }
    }

    void handle_keys(pollfd& fd) {
        char buf[64];
        const ssize_t n = read(STDIN_FILENO, buf, sizeof buf);
        if (n > 0)
            toggle_pause();
        else if (n == 0 || (errno != EINTR && errno != EAGAIN))
            fd.fd = -1;
    }

    void toggle_pause() {
        const bool paused = !g_paused;
        g_paused = paused;
        std::printf("%s: %s\n", name_, paused ? "paused" : "resumed");
        std::fflush(stdout);
    }

    const char* name_;
    std::function<void()> on_tick_;
    int pipe_[2] = {-1, -1};
    termios saved_terminal_{};
    bool terminal_saved_ = false;
    std::thread thread_;
};

}
