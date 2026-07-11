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

// EPOLLIN	可读	fd 有数据可读
// EPOLLOUT	可写	fd 发送缓冲区有空闲
// EPOLLERR	错误	fd 发生错误（如连接重置）
// EPOLLHUP	挂断	对端关闭连接（写端）
// EPOLLPRI	紧急数据	带外数据（几乎不用）
// EPOLLRDHUP	对端关闭读端	Linux 2.6.17+，检测对端 shutdown 读端


void Channel::handleEvent() {  //事件分发器  & 位与
    std::cout << "handleEvent revents=" << revents_ << std::endl;  // ← 加这行
    // EPOLLHUP 可能和 EPOLLIN 同时触发（对端发完数据再关闭）。如果有数据可读，先处理读，再处理关闭。避免丢数据。
    // EPOLLHUP 处理加了 !EPOLLIN 条件，避免和读事件冲突。有数据时优先读，读到 EOF 再关闭；没数据时直接触发关闭回调。这是 Reactor 模式的事件分发顺序设计。
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {  //关闭连接
        if (closeCallback_) closeCallback_();
    }
    
    if (revents_ & EPOLLERR) {  //错误事件
        if (errorCallback_) errorCallback_();
    }
    
    // 检测对端调用 shutdown(fd, SHUT_RD) 或半关闭。你的代码里把它和 EPOLLIN 放一起：
    // EPOLLRDHUP 也触发读回调。通常 EPOLLRDHUP 应该触发关闭回调，但这里和读放一起，可能是因为对端半关闭后可能还有数据要读。
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {   //可读事件
        if (readCallback_) readCallback_();
    }
    
    if (revents_ & EPOLLOUT) {  //可写事件
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