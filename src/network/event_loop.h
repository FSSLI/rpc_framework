// src/network/event_loop.h
#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <functional>
#include <vector>
#include <memory>
#include <sys/epoll.h>
#include <cassert>
#include <mutex>
#include <queue>
#include <chrono>  // ← 新增

namespace rpc {

class Channel;

// 定时器任务
struct TimerTask {
    std::chrono::steady_clock::time_point nextRun;
    std::chrono::milliseconds interval;
    std::function<void()> callback;
    
    bool operator>(const TimerTask& other) const {
        return nextRun > other.nextRun;
    }
};

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
    
    // 新增：定时任务
    void runEvery(double intervalSeconds, std::function<void()> cb);

    void assertInLoopThread();

private:
    static const int kEpollWaitTimeoutMs = 10000;
    
    int epollfd_;
    std::vector<struct epoll_event> events_;
    bool looping_;
    bool quit_;
    
    int wakeupFd_;
    std::unique_ptr<Channel> wakeupChannel_;
    
    std::vector<std::function<void()>> pendingFunctors_;
    std::mutex pendingMutex_;
    
    // 新增：定时器
    std::priority_queue<TimerTask, std::vector<TimerTask>, std::greater<TimerTask>> timers_;
    
    void doPendingFunctors();
    void wakeup();
    void handleRead();
    void processTimers();  // 新增
};

} // namespace rpc

#endif