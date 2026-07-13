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
#include <chrono>
#include <atomic>
#include <unordered_map>

namespace rpc {

class Channel;

// 定时器任务
struct TimerTask {
    uint64_t id;  // 唯一ID，用于取消
    std::chrono::steady_clock::time_point nextRun;
    std::chrono::milliseconds interval;
    std::function<void()> callback;
    std::shared_ptr<bool> cancelled;  // Issue #4 fix: 取消标志，避免 cancelledTimers_ 集合泄漏

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

    // 定时任务，返回 task id 用于取消
    uint64_t runEvery(double intervalSeconds, std::function<void()> cb);
    uint64_t runAfter(double delaySeconds, std::function<void()> cb);
    void cancelTimer(uint64_t taskId);

    void assertInLoopThread();

private:
    static const int kEpollWaitTimeoutMs = 10000;

    int epollfd_;
    std::vector<struct epoll_event> events_;
    bool looping_;
    std::atomic<bool> quit_;

    int wakeupFd_;
    std::unique_ptr<Channel> wakeupChannel_;

    std::vector<std::function<void()>> pendingFunctors_;
    std::mutex pendingMutex_;

    // 定时器
    std::priority_queue<TimerTask, std::vector<TimerTask>, std::greater<TimerTask>> timers_;
    mutable std::mutex timerMutex_;
    uint64_t nextTimerId_ = 1;
    // Issue #4 fix: 用 shared_ptr<bool> 替代 ID 集合，定时器到期自动清理
    std::unordered_map<uint64_t, std::shared_ptr<bool>> timerFlags_;

    void doPendingFunctors();
    void wakeup();
    void handleRead();
    void processTimers();
    int getNextTimerTimeoutMs() const;
};

} // namespace rpc
#endif