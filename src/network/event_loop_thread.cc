// src/network/event_loop_thread.cc
#include "event_loop_thread.h"
#include "event_loop.h"

namespace rpc {

EventLoopThread::EventLoopThread(const ThreadInitCallback& cb)  //构造函数传递初始化回调函数
    : loop_(nullptr),
      exiting_(false),
      thread_(),
      mutex_(),
      cond_(),
      callback_(cb) {
}

EventLoopThread::~EventLoopThread() {
    if (!exiting_) {
        stop();
    }
    // Issue #9 fix: 如果在 loop 线程调了 stop()（同线程不 join），析构时线程
    // 已因 quit() 而自然退出，join 安全；若尚未退出，detach 防止 ~thread terminate
    if (thread_.joinable()) {
        if (thread_.get_id() != std::this_thread::get_id()) {
            thread_.join();
        } else {
            thread_.detach();  // 理论上不会到这里，防御
        }
    }
}

EventLoop* EventLoopThread::startLoop() {  // 启动线程，创建 EventLoop，返回指针
    thread_ = std::thread(&EventLoopThread::threadFunc, this);   // 创建子线程 子线程执行threadFunc

    EventLoop* loop = nullptr;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (loop_ == nullptr) {
            cond_.wait(lock);  
        }
        loop = loop_;
    }
    return loop;
}

void EventLoopThread::stop() {
    exiting_ = true;
    if (loop_ != nullptr) {
        loop_->quit();
    }
    // Issue #9 fix: 同线程不能 join（死锁），也不能 detach（~thread 会 terminate）。
    // quit() 后 loop 自然退出 → threadFunc 结束 → 线程自行终止。
    // 让 ~EventLoopThread() 处理 join 或 detach。
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) {
        thread_.join();
    } else if (thread_.joinable()) {
        // FIX: 同线程不能 join 自己，必须 detach，否则析构时 terminate
        thread_.detach();
    }
    loop_ = nullptr;
    // 同线程调用：不 join 不 detach，留给析构函数处理
}

void EventLoopThread::threadFunc() {  //子线程的入口函数。
    EventLoop loop;

    if (callback_) {  //有初始化就初始化
        callback_(&loop);
    }

    {
        std::unique_lock<std::mutex> lock(mutex_);  //加锁返回loop指针
        loop_ = &loop;
        cond_.notify_one();  // 通知主线程
    }

    loop.loop();  //threadFunc 在子线程执行，loop.loop() 也在子线程阻塞。

    std::unique_lock<std::mutex> lock(mutex_);  //循环结束释放loop
    loop_ = nullptr;
}

} // namespace rpc