// src/network/event_loop.h
#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <functional>
#include <vector>
#include <memory>
#include <sys/epoll.h>
#include <cassert>  // ← 加这行，assert 需要

namespace rpc {

class Channel;

class EventLoop {
public:
    EventLoop();
    ~EventLoop();
    
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;
    
    void loop();
    void quit();
    
    void updateChannel(Channel* channel);
    void removeChannel(Channel* channel);
    
    bool isInLoopThread() const;

    void runInLoop(std::function<void()> cb);
    void queueInLoop(std::function<void()> cb);

    void assertInLoopThread();  // ← 加这行

private:
    static const int kEpollWaitTimeoutMs = 10000;
    
    int epollfd_;
    std::vector<struct epoll_event> events_;
    bool looping_;
    bool quit_;
    
    int wakeupFd_;
    std::unique_ptr<Channel> wakeupChannel_;
    
    std::vector<std::function<void()>> pendingFunctors_;
    
    void doPendingFunctors();
    void wakeup();
    void handleRead();  // ← 加这行
    
};

} // namespace rpc

#endif