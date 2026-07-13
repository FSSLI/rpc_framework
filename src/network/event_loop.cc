// src/network/event_loop.cc
#include "network/event_loop.h"
#include "network/channel.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <cstdlib>
#include <iostream>
#include <limits>

namespace rpc {

__thread EventLoop* t_loopInThisThread = nullptr;

EventLoop::EventLoop()
    : epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(16),
      looping_(false),
      quit_(false),
      wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      nextTimerId_(1) {

    if (t_loopInThisThread) {
        abort();
    } else {
        t_loopInThisThread = this;
    }

    if (epollfd_ < 0) {
        abort();
    }

    if (wakeupFd_ < 0) {
        abort();
    }

    wakeupChannel_ = std::make_unique<Channel>(this, wakeupFd_);
    wakeupChannel_->setReadCallback(std::bind(&EventLoop::handleRead, this));
    wakeupChannel_->enableReading();
}

EventLoop::~EventLoop() {
    wakeupChannel_->disableAll();
    wakeupChannel_->remove();
    ::close(wakeupFd_);
    ::close(epollfd_);
    t_loopInThisThread = nullptr;
}

void EventLoop::loop() {
    assert(!looping_);
    assertInLoopThread();

    looping_ = true;
    quit_.store(false);

    while (!quit_.load()) {
        int timeoutMs = getNextTimerTimeoutMs();

        int numEvents = ::epoll_wait(epollfd_, &*events_.begin(),
                                      static_cast<int>(events_.size()),
                                      timeoutMs);

        if (numEvents > 0) {
            for (int i = 0; i < numEvents; ++i) {
                Channel* channel = static_cast<Channel*>(events_[i].data.ptr);
                channel->set_revents(events_[i].events);
                // Issue #2 fix: 异常不跳出事件循环，保护同线程其他连接
                try {
                    channel->handleEvent();
                } catch (const std::exception& e) {
                    std::cerr << "EventLoop channel exception: " << e.what() << std::endl;
                } catch (...) {
                    std::cerr << "EventLoop channel unknown exception" << std::endl;
                }
            }
            if (static_cast<size_t>(numEvents) == events_.size()) {
                events_.resize(events_.size() * 2);
            }
        } else if (numEvents == 0) {
            // timeout or no events
        } else {
            if (errno != EINTR) {
                // LOG_SYSERR
            }
        }

        processTimers();
        doPendingFunctors();
    }

    looping_ = false;
}

int EventLoop::getNextTimerTimeoutMs() const {
    std::lock_guard<std::mutex> lock(timerMutex_);
    if (timers_.empty()) {
        return kEpollWaitTimeoutMs;
    }

    auto now = std::chrono::steady_clock::now();
    auto nextRun = timers_.top().nextRun;

    if (nextRun <= now) {
        return 0;  // 有定时器已到期，立即返回
    }

    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(nextRun - now).count();
    if (diff > kEpollWaitTimeoutMs) {
        return kEpollWaitTimeoutMs;
    }
    return static_cast<int>(diff);
}

void EventLoop::processTimers() {
    // Issue #3 fix: 先取锁收集到期任务，再放锁执行回调，防止回调中调 runAfter/runEvery 死锁
    std::vector<TimerTask> expiredTasks;
    {
        std::lock_guard<std::mutex> lock(timerMutex_);
        auto now = std::chrono::steady_clock::now();

        while (!timers_.empty() && timers_.top().nextRun <= now) {
            TimerTask task = timers_.top();
            timers_.pop();

            bool cancelled = task.cancelled && *task.cancelled;
            if (cancelled) {
                timerFlags_.erase(task.id);
                continue;
            }

            // 一次性任务：擦除追踪条目；周期性任务：保留条目供 cancelTimer 继续使用
            if (task.interval.count() == 0) {
                timerFlags_.erase(task.id);
            }
            expiredTasks.push_back(std::move(task));
        }
    }

    // 锁外执行回调
    for (auto& task : expiredTasks) {
        // Issue #2 fix: 定时器回调异常不退出循环
        try {
            task.callback();
        } catch (const std::exception& e) {
            std::cerr << "EventLoop timer exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "EventLoop timer unknown exception" << std::endl;
        }

        // Issue #4 fix: 用局部变量 move，避免对循环引用变量 move 造成混淆
        if (task.interval.count() > 0) {
            std::lock_guard<std::mutex> lock(timerMutex_);
            TimerTask renewed = std::move(task);
            renewed.nextRun = renewed.nextRun + renewed.interval;
            timers_.push(std::move(renewed));
        }
    }
}

uint64_t EventLoop::runEvery(double intervalSeconds, std::function<void()> cb) {
    if (intervalSeconds <= 0) intervalSeconds = 0.001;  // Issue #4: Release 安全兜底
    std::lock_guard<std::mutex> lock(timerMutex_);
    auto cancelled = std::make_shared<bool>(false);
    uint64_t id = nextTimerId_++;
    timerFlags_[id] = cancelled;

    TimerTask task;
    task.id = id;
    task.nextRun = std::chrono::steady_clock::now() +
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::duration<double>(intervalSeconds));
    task.interval = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::duration<double>(intervalSeconds));
    task.callback = std::move(cb);
    task.cancelled = cancelled;
    timers_.push(std::move(task));

    // 如果有新定时器比当前最近的还早，唤醒 epoll_wait 重新计算超时
    if (!isInLoopThread()) {
        wakeup();
    }
    return id;
}

uint64_t EventLoop::runAfter(double delaySeconds, std::function<void()> cb) {
    if (delaySeconds < 0) delaySeconds = 0;  // Issue #4: Release 安全兜底
    std::lock_guard<std::mutex> lock(timerMutex_);
    auto cancelled = std::make_shared<bool>(false);
    uint64_t id = nextTimerId_++;
    timerFlags_[id] = cancelled;

    TimerTask task;
    task.id = id;
    task.nextRun = std::chrono::steady_clock::now() +
                   std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::duration<double>(delaySeconds));
    task.interval = std::chrono::milliseconds(0);
    task.callback = std::move(cb);
    task.cancelled = cancelled;
    timers_.push(std::move(task));

    if (!isInLoopThread()) {
        wakeup();
    }
    return id;
}

void EventLoop::cancelTimer(uint64_t taskId) {
    std::lock_guard<std::mutex> lock(timerMutex_);
    // Issue #4 fix: 设置取消标志，定时器到期时自动跳过并清理
    auto it = timerFlags_.find(taskId);
    if (it != timerFlags_.end()) {
        *it->second = true;
    }
}

void EventLoop::quit() {
    quit_.store(true);
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::updateChannel(Channel* channel) {
    assertInLoopThread();

    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = channel->events();
    event.data.ptr = channel;

    int fd = channel->fd();
    int index = channel->index();

    if (index == -1 || index == 2) {
        if (::epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &event) < 0) {
            // LOG_SYSERR
        }
        channel->set_index(1);
    } else {
        if (channel->isNoneEvent()) {
            if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, &event) < 0) {
                // LOG_SYSERR
            }
            channel->set_index(2);
        } else {
            if (::epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &event) < 0) {
                // LOG_SYSERR
            }
        }
    }
}

void EventLoop::removeChannel(Channel* channel) {
    assertInLoopThread();

    int fd = channel->fd();
    int index = channel->index();

    if (index == 1) {
        if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
            // LOG_SYSERR
        }
    }
    channel->set_index(-1);
}

bool EventLoop::isInLoopThread() const {
    return this == t_loopInThisThread;
}

void EventLoop::assertInLoopThread() {
    if (!isInLoopThread()) {
        abort();
    }
}

void EventLoop::runInLoop(std::function<void()> cb) {
    if (isInLoopThread()) {
        cb();
    } else {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(std::function<void()> cb) {
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingFunctors_.push_back(std::move(cb));
    }
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::doPendingFunctors() {
    std::vector<std::function<void()>> functors;
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        functors.swap(pendingFunctors_);
    }

    for (const auto& f : functors) {
        // Issue #2 fix: pending functor 异常不退出循环
        try {
            f();
        } catch (const std::exception& e) {
            std::cerr << "EventLoop functor exception: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "EventLoop functor unknown exception" << std::endl;
        }
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        // LOG_ERROR
    }
}

void EventLoop::handleRead() {
    uint64_t one;
    ssize_t n = ::read(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        // LOG_ERROR
    }
}

} // namespace rpc