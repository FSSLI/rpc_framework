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
    // Issue #6 fix: 只要曾注册过就尝试移除，覆盖 index_==2（已标记删除）场景
    if (index_ != -1) {
        remove();
    }
}

void Channel::handleEvent() {
    std::cout << "handleEvent revents=" << revents_ << std::endl;

    // EPOLLERR 通常意味着连接出现致命错误，应该关闭
    if (revents_ & EPOLLERR) {
        if (errorCallback_) errorCallback_();
        // ERR 后触发关闭，避免死连接
        if (closeCallback_) closeCallback_();
        return;
    }

    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (closeCallback_) closeCallback_();
    }

    if (revents_ & (EPOLLIN | EPOLLPRI)) {
        if (readCallback_) readCallback_();
    }

    // EPOLLRDHUP：对端关闭写端，但可能还有数据要读
    // 先处理读，再处理 RDHUP
    if (revents_ & EPOLLRDHUP) {
        if (readCallback_) readCallback_();  // 尝试读剩余数据
        if (closeCallback_) closeCallback_();
    }

    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }
}

void Channel::enableReading() {  
    events_ |= kReadEvent;
    update();
    std::cout << "enableReading fd=" << fd_ << " events=" << events_ << " index=" << index_ << std::endl;
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