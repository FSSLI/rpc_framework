// src/network/event_loop_thread.h
#ifndef EVENT_LOOP_THREAD_H
#define EVENT_LOOP_THREAD_H

#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace rpc {

class EventLoop;

class EventLoopThread {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;  

    EventLoopThread(const ThreadInitCallback& cb = ThreadInitCallback());  //初始化时注册初始化回调函数
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    EventLoop* startLoop();  // 启动线程，创建 EventLoop，返回 EventLoop 指针
    void stop();  // 停止线程，调用 loop->quit()，等待线程结束

private:
    void threadFunc();   // 线程入口函数：创建 EventLoop，执行 callback，启动 loop()

    EventLoop* loop_;  // 指向线程内创建的 EventLoop，startLoop() 返回的就是它
    bool exiting_;  //是否退出
    std::thread thread_;  //对应线程对象
    std::mutex mutex_;  //锁
    std::condition_variable cond_;  //条件变量
    ThreadInitCallback callback_;  // 线程初始化回调，loop() 开始前执行
};

} // namespace rpc

#endif