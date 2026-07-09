// src/network/event_loop.cc
#include "event_loop.h"
#include "channel.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <cstdlib>
#include <iostream>

namespace rpc {

// __thread 线程局部变量，确保一个线程只有一个 EventLoop
__thread EventLoop* t_loopInThisThread = nullptr;

EventLoop::EventLoop()
    : epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(16),
      looping_(false),
      quit_(false),
      wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      pendingFunctors_() {
    
    // 先设置线程局部变量
    if (t_loopInThisThread) {
        // LOG_FATAL << "Another EventLoop exists in this thread";
        abort();  // 或者抛异常
    } else {
        t_loopInThisThread = this;  // ← 移到最前面
    }
    
    if (epollfd_ < 0) {
        // LOG_FATAL
    }
    
    if (wakeupFd_ < 0) {
        // LOG_FATAL
    }
    
    wakeupChannel_ = std::make_unique<Channel>(this, wakeupFd_);
    wakeupChannel_->setReadCallback(std::bind(&EventLoop::handleRead, this));
    wakeupChannel_->enableReading();  // 现在 t_loopInThisThread 已经设置了
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
        events_.resize(16);  // 每次重置，Channel::handleEvent 会动态扩容
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
                events_.resize(events_.size() * 2);  // 扩容
            }
        } else if (numEvents == 0) {
            // timeout，正常
        } else {
            if (errno != EINTR) {
                // LOG_SYSERR << "EventLoop::loop() epoll_wait";
            }
        }
        
        // 执行回调
        doPendingFunctors();
    }
    
    looping_ = false;
}

void EventLoop::quit() {
    quit_ = true;
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::updateChannel(Channel* channel) {
    assertInLoopThread();
    std::cout << "updateChannel fd=" << channel->fd() << " index=" << channel->index() << std::endl;  // ← 加
    
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = channel->events();
    event.data.ptr = channel;
    
    int fd = channel->fd();
    int index = channel->index();
    
    if (index == -1 || index == 2) {  // kNew = -1, kDeleted = 2
        if (index == -1) {
            // 新 Channel
        }
        if (::epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &event) < 0) {
            // LOG_SYSERR << "epoll_ctl add";
        }
        channel->set_index(1);  // kAdded
    } else {
        // 已存在，修改
        if (channel->isNoneEvent()) {
            if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, &event) < 0) {
                // LOG_SYSERR << "epoll_ctl del";
            }
            channel->set_index(2);  // kDeleted
        } else {
            if (::epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &event) < 0) {
                // LOG_SYSERR << "epoll_ctl mod";
            }
        }
    }
}

void EventLoop::removeChannel(Channel* channel) {
    assertInLoopThread();
    
    int fd = channel->fd();
    int index = channel->index();
    
    if (index == 1) {  // kAdded
        if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {
            // LOG_SYSERR << "epoll_ctl del";
        }
    }
    channel->set_index(-1);  // kNew
}

bool EventLoop::isInLoopThread() const {
    return this == t_loopInThisThread;
}

void EventLoop::assertInLoopThread() {
    if (!isInLoopThread()) {
        // LOG_FATAL << "not in loop thread";
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
        // TODO: 加锁保护 pendingFunctors_
        pendingFunctors_.push_back(std::move(cb));
    }
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::doPendingFunctors() {
    std::vector<std::function<void()>> functors;
    {
        // TODO: 加锁，swap 出来减少锁持有时间
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
        // LOG_ERROR << "EventLoop::wakeup() writes " << n << " bytes";
    }
}

void EventLoop::handleRead() {
    uint64_t one;
    ssize_t n = ::read(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        // LOG_ERROR << "EventLoop::handleRead() reads " << n << " bytes";
    }
}

} // namespace rpc