// src/network/event_loop.cc
#include "network/event_loop.h"
#include "network/channel.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <cstdlib>
#include <iostream>

namespace rpc {

__thread EventLoop* t_loopInThisThread = nullptr;

EventLoop::EventLoop()
    : epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(16),
      looping_(false),
      quit_(false),
      wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
    
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
    quit_ = false;
    
    while (!quit_) {
        events_.resize(16);
        int numEvents = ::epoll_wait(epollfd_, &*events_.begin(), 
                                      static_cast<int>(events_.size()),
                                      kEpollWaitTimeoutMs);
        
        if (numEvents > 0) {
            for (int i = 0; i < numEvents; ++i) {
                Channel* channel = static_cast<Channel*>(events_[i].data.ptr);
                channel->set_revents(events_[i].events);
                channel->handleEvent();
            }
            if (static_cast<size_t>(numEvents) == events_.size()) {
                events_.resize(events_.size() * 2);
            }
        } else if (numEvents == 0) {
            // timeout
        } else {
            if (errno != EINTR) {
                // LOG_SYSERR
            }
        }
        
        // 执行定时器
        processTimers();
        
        // 执行回调
        doPendingFunctors();
    }
    
    looping_ = false;
}

void EventLoop::processTimers() {
    auto now = std::chrono::steady_clock::now();
    while (!timers_.empty() && timers_.top().nextRun <= now) {
        auto task = timers_.top();
        timers_.pop();
        task.callback();
        // 重新加入队列（周期性执行）
        task.nextRun = now + task.interval;
        timers_.push(task);
    }
}

void EventLoop::runEvery(double intervalSeconds, std::function<void()> cb) {
    assertInLoopThread();
    TimerTask task;
    task.nextRun = std::chrono::steady_clock::now() + 
                   std::chrono::milliseconds(static_cast<int>(intervalSeconds * 1000));
    task.interval = std::chrono::milliseconds(static_cast<int>(intervalSeconds * 1000));
    task.callback = std::move(cb);
    timers_.push(std::move(task));
}

// ... 其他代码不变 ...

void EventLoop::quit() {
    quit_ = true;
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
        f();
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