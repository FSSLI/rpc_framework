// src/network/channel.h
#ifndef CHANNEL_H
#define CHANNEL_H

#include <functional>
#include <sys/epoll.h>  // ← 加这行

namespace rpc {

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel();

    void handleEvent();

    void setReadCallback(EventCallback cb) { readCallback_ = std::move(cb); }
    void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }

    int fd() const { return fd_; }
    int events() const { return events_; }
    void set_revents(int revt) { revents_ = revt; }

    void enableReading();
    void disableReading();
    void enableWriting();
    void disableWriting();
    void disableAll();

    bool isWriting() const;
    bool isReading() const;
    bool isNoneEvent() const;  // ← 加这行

    int index() const { return index_; }
    void set_index(int idx) { index_ = idx; }

    EventLoop* ownerLoop() { return loop_; }
    
    void remove();  // ← 加这行

private:
    static const int kNoneEvent = 0;
    static const int kReadEvent = EPOLLIN | EPOLLPRI;
    static const int kWriteEvent = EPOLLOUT;

    EventLoop* loop_;
    const int fd_;
    int events_;
    int revents_;
    int index_;

    EventCallback readCallback_;
    EventCallback writeCallback_;
    EventCallback errorCallback_;
    EventCallback closeCallback_;

    void update();  // ← 加这行，private 方法
};

} // namespace rpc

#endif