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
    std::chrono::steady_clock::time_point nextRun;  // 下次执行时间
    std::chrono::milliseconds interval;              // 间隔（周期任务）  0 表示一次性
    std::function<void()> callback;                    // 回调
    
    bool operator>(const TimerTask& other) const {
        return nextRun > other.nextRun;
    }
};

class EventLoop {
public:
    EventLoop();
    ~EventLoop();
    
    //禁止拷贝
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;
    
    void loop();  //开始循环
    void quit();  //退出
    
    void updateChannel(Channel* channel);  //创建/更新channel到epoll
    void removeChannel(Channel* channel);  //移除epoll的channel
    
    bool isInLoopThread() const;  //是否在当前线程里

    void runInLoop(std::function<void()> cb);  //在自己的线程运行
    void queueInLoop(std::function<void()> cb);  //投递给其他线程
    
    // 新增：定时任务
    void runEvery(double intervalSeconds, std::function<void()> cb);  //设置定时

    // 一次性任务
    void runAfter(double delaySeconds, std::function<void()> cb);

    void assertInLoopThread();  //断言当前线程是 EventLoop 所属线程，否则 abort

private:
    static const int kEpollWaitTimeoutMs = 10000;
    
    int epollfd_;  //epoll实例
    std::vector<struct epoll_event> events_;  //epoll返回的事件
    bool looping_;  //运行标志位置
    bool quit_;  //退出标志位
    
    int wakeupFd_;  //// eventfd，其他线程写数据唤醒 epoll_wait
    std::unique_ptr<Channel> wakeupChannel_;  //对应的channel，绑定wakeupFd_，注册读事件
    
    std::vector<std::function<void()>> pendingFunctors_;  //其他线程投递过来的回调函数
    std::mutex pendingMutex_;  //保护 pendingFunctors_，queueInLoop 和 doPendingFunctors 可能跨线程竞争
    
    // 新增：定时器
    std::priority_queue<TimerTask, std::vector<TimerTask>, std::greater<TimerTask>> timers_;  //定时器优先队列，按下次执行时间排序。
    
    void doPendingFunctors();  // 执行 pendingFunctors_ 里的回调（用 swap 减少锁持有时间）
    void wakeup();  // 往 wakeupFd_ 写数据，唤醒 epoll_wait
    void handleRead();  // wakeupChannel_ 的读回调，读走 wakeupFd_ 数据
    void processTimers();  // 检查并执行到期的定时器任务
};

} // namespace rpc

#endif