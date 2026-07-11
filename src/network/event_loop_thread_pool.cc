// src/network/event_loop_thread_pool.cc
#include "event_loop_thread_pool.h"
#include "event_loop_thread.h"
#include "event_loop.h"

namespace rpc {

EventLoopThreadPool::EventLoopThreadPool(EventLoop* baseLoop, const std::string& name)
    : baseLoop_(baseLoop),
      name_(name),
      started_(false),
      numThreads_(0),  //默认为一个主线程处理
      next_(0) {
}

EventLoopThreadPool::~EventLoopThreadPool() {  //这里不操作吗？
    // ~EventLoopThreadPool() 时，unique_ptr 自动 delete 每个 EventLoopThread
    // EventLoopThread 的析构函数会 quit + join
}

void EventLoopThreadPool::start(const ThreadInitCallback& cb) { //没看懂
    started_ = true;

    // 创建 numThreads_ 个子线程
    for (int i = 0; i < numThreads_; ++i) {
        char buf[name_.size() + 32];
        snprintf(buf, sizeof(buf), "%s%d", name_.c_str(), i);  // 线程名：如 "IOThread0"
        EventLoopThread* t = new EventLoopThread(cb);  // 创建线程对象
        threads_.push_back(std::unique_ptr<EventLoopThread>(t));  // unique_ptr 管理
        loops_.push_back(t->startLoop());    // 启动线程，返回 EventLoop 指针
    }

    if (numThreads_ == 0 && cb) {  // 单线程模式：直接在 baseLoop 执行初始化回调
        cb(baseLoop_);
    }
}

EventLoop* EventLoopThreadPool::getNextLoop() {  
    baseLoop_->assertInLoopThread();  //getNextLoop 必须在 baseLoop 线程调用，assert 防止跨线程竞争 next_
    EventLoop* loop = baseLoop_;  // 默认返回 baseLoop

    if (!loops_.empty()) { 
        loop = loops_[next_];  // 有子线程才分配
        ++next_;
        if (static_cast<size_t>(next_) >= loops_.size()) {
            next_ = 0;
        }
    }
    return loop;
}

EventLoop* EventLoopThreadPool::getLoopForHash(size_t hashCode) {
    baseLoop_->assertInLoopThread();
    EventLoop* loop = baseLoop_;

    if (!loops_.empty()) {
        loop = loops_[hashCode % loops_.size()];
    }
    return loop;
}

} // namespace rpc