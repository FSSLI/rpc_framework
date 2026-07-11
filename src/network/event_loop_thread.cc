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
    exiting_ = true;
    if (loop_ != nullptr) {  //loop退出，线程对象join等待线程函数退出，回收资源
        loop_->quit();
        thread_.join();  // 等待子线程结束
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
    if (thread_.joinable()) {
        thread_.join();
    }
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