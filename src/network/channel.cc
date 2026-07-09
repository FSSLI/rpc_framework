// src/network/channel.cc
#include "channel.h"
#include "event_loop.h"
#include <sys/epoll.h>
#include <iostream>

namespace rpc {

const int Channel::kNoneEvent;
const int Channel::kReadEvent;
const int Channel::kWriteEvent;

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      events_(0),
      revents_(0),
      index_(-1) {
}

Channel::~Channel() {
    // 确保已经从 EventLoop 中移除
}

void Channel::handleEvent() {
    std::cout << "handleEvent revents=" << revents_ << std::endl;  // ← 加这行
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (closeCallback_) closeCallback_();
    }
    
    if (revents_ & EPOLLERR) {
        if (errorCallback_) errorCallback_();
    }
    
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        if (readCallback_) readCallback_();
    }
    
    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }
}

void Channel::enableReading() {
    events_ |= kReadEvent;
    update();
    std::cout << "enableReading fd=" << fd_ << " events=" << events_ << " index=" << index_ << std::endl;  // ← 加
}

void Channel::disableReading() {
    events_ &= ~kReadEvent;
    update();
}

void Channel::enableWriting() {
    events_ |= kWriteEvent;
    update();
}

void Channel::disableWriting() {
    events_ &= ~kWriteEvent;
    update();
}

void Channel::disableAll() {
    events_ = kNoneEvent;
    update();
}

bool Channel::isWriting() const {
    return events_ & kWriteEvent;
}

bool Channel::isReading() const {
    return events_ & kReadEvent;
}

bool Channel::isNoneEvent() const {
    return events_ == kNoneEvent;
}

void Channel::update() {
    loop_->updateChannel(this);
}

void Channel::remove() {
    loop_->removeChannel(this);
}

} // namespace rpc