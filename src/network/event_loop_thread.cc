// src/network/event_loop_thread.cc
#include "event_loop_thread.h"
#include "event_loop.h"

namespace rpc {

EventLoopThread::EventLoopThread(const ThreadInitCallback& cb)
    : loop_(nullptr),
      exiting_(false),
      thread_(),
      mutex_(),
      cond_(),
      callback_(cb) {
}

EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    if (loop_ != nullptr) {
        loop_->quit();
        thread_.join();
    }
}

EventLoop* EventLoopThread::startLoop() {
    thread_ = std::thread(&EventLoopThread::threadFunc, this);

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

void EventLoopThread::threadFunc() {
    EventLoop loop;

    if (callback_) {
        callback_(&loop);
    }

    {
        std::unique_lock<std::mutex> lock(mutex_);
        loop_ = &loop;
        cond_.notify_one();
    }

    loop.loop();

    std::unique_lock<std::mutex> lock(mutex_);
    loop_ = nullptr;
}

} // namespace rpc