// src/network/event_loop.cc
#include "network/event_loop.h"
#include "network/channel.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <cstdlib>
#include <iostream>

namespace rpc {

// __thread 是 GCC 的线程局部存储（TLS）关键字。

__thread EventLoop* t_loopInThisThread = nullptr;  //记录当前线程的eventloop是谁

// epoll_create1 是 epoll_create 的增强版，支持 flags：
// EPOLL_CLOEXEC：设置 close-on-exec，子进程不会继承这个 fd
// 避免 fork + exec 后 fd 泄漏。

EventLoop::EventLoop()
    : epollfd_(::epoll_create1(EPOLL_CLOEXEC)),  //创建epoll实例，
      events_(16),  //默认接收16个事件吗？
      looping_(false),  //未开始循环
      quit_(false),  //未退出
      wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {  //创建wakeupFd
    
    if (t_loopInThisThread) {   //避免多次启动吗？
        abort();  // 这个线程已经有 EventLoop 了，不能创建第二个
    } else {
        t_loopInThisThread = this;  //绑定线程IP
    }
    
    if (epollfd_ < 0) {  //创建epoll失败
        abort();
    }
    
    if (wakeupFd_ < 0) {  //创建wakeupFd失败
        abort();
    }
    
    wakeupChannel_ = std::make_unique<Channel>(this, wakeupFd_);  //绑定channel
    wakeupChannel_->setReadCallback(std::bind(&EventLoop::handleRead, this));  //注册读回调函数
    wakeupChannel_->enableReading();  //注册读事件到epoll
}

EventLoop::~EventLoop() {
    wakeupChannel_->disableAll();  //注销所有事件
    wakeupChannel_->remove();  //删除channel
    ::close(wakeupFd_);  //关闭fd
    ::close(epollfd_);  //关闭epoll实例
    t_loopInThisThread = nullptr;  //解绑
}

void EventLoop::loop() {
    assert(!looping_);  //避免重复启动
    assertInLoopThread();  //确保是在自己的线程上执行循环
    
    looping_ = true; 
    quit_ = false;
    
    while (!quit_) {
        events_.resize(16);  //重置epoll返回的事件
        int numEvents = ::epoll_wait(epollfd_, &*events_.begin(), 
                                      static_cast<int>(events_.size()),
                                      kEpollWaitTimeoutMs);  //超时等待事件返回
        
        if (numEvents > 0) {  //有事件返回
            for (int i = 0; i < numEvents; ++i) {  //遍历事件并且执行
                Channel* channel = static_cast<Channel*>(events_[i].data.ptr);  //获取channel指针
                channel->set_revents(events_[i].events);  //设置实际发生事件
                channel->handleEvent();  //执行回调函数
            }
            if (static_cast<size_t>(numEvents) == events_.size()) {  //扩容
                events_.resize(events_.size() * 2);
            }
        } else if (numEvents == 0) {  //超时无事件返回
            // timeout
        } else {
            if (errno != EINTR) {  //报错
                // LOG_SYSERR
            }
        }
        
        // 执行定时器
        processTimers();  //执行定时器回调函数
        
        // 执行回调
        doPendingFunctors();  //执行其他线程投递的回调函数
    }
    
    looping_ = false;
}

void EventLoop::processTimers() {
    auto now = std::chrono::steady_clock::now();
    while (!timers_.empty() && timers_.top().nextRun <= now) {
        auto task = timers_.top();
        timers_.pop();
        task.callback();
        // 周期性任务重新入队，一次性任务丢弃
        if (task.interval.count() > 0) {
            task.nextRun = task.nextRun + task.interval;  // 固定间隔
            timers_.push(task);
        }
    }
}

void EventLoop::runEvery(double intervalSeconds, std::function<void()> cb) {  //注册定时器任务
    assertInLoopThread();
    TimerTask task;
    task.nextRun = std::chrono::steady_clock::now() + 
                   std::chrono::milliseconds(static_cast<int>(intervalSeconds * 1000));
    task.interval = std::chrono::milliseconds(static_cast<int>(intervalSeconds * 1000));
    task.callback = std::move(cb);
    timers_.push(std::move(task));  //放入优先队列
}

void EventLoop::runAfter(double delaySeconds, std::function<void()> cb) {
    assertInLoopThread();
    TimerTask task;
    task.nextRun = std::chrono::steady_clock::now() + 
                   std::chrono::milliseconds(static_cast<int>(delaySeconds * 1000));
    task.interval = std::chrono::milliseconds(0);  // 0 表示一次性
    task.callback = std::move(cb);
    timers_.push(std::move(task));
}

// ... 其他代码不变 ...

void EventLoop::quit() {  //设置当前 EventLoop 的 quit_
    quit_ = true;  // 设置**当前对象**的 quit_
    if (!isInLoopThread()) {
        wakeup();  // 如果调用者不是 EventLoop 所属线程，需要唤醒
    }
}

void EventLoop::updateChannel(Channel* channel) {  //注册channel
    assertInLoopThread();
    
    struct epoll_event event;
    memset(&event, 0, sizeof(event));  //清零 epoll_event 结构体，避免未初始化字段有垃圾值。
    event.events = channel->events();  //绑定感兴趣事件
    event.data.ptr = channel;  //绑定channel指针
    
    int fd = channel->fd();  //获取文件描述符
    int index = channel->index();  //获取channel状态
    
    if (index == -1 || index == 2) {  // 未注册或已删除，重新 ADD
        if (::epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &event) < 0) {
            // LOG_SYSERR
        }
        channel->set_index(1); //添加状态？
    } else { // 已注册
        if (channel->isNoneEvent()) {  // 没有事件了，DEL
            if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, &event) < 0) {  //updateChannel 里的 DEL：channel->isNoneEvent() 时调用
                // LOG_SYSERR
            }
            channel->set_index(2);
        } else {  // 修改事件，MOD
            if (::epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &event) < 0) {
                // LOG_SYSERR
            }
        }
    }
}

void EventLoop::removeChannel(Channel* channel) {  
    assertInLoopThread();
    
    int fd = channel->fd();
    int index = channel->index();
    
    if (index == 1) {  //removeChannel 里的 DEL：显式移除
        if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, nullptr) < 0) {  //和上面的删除不太一样
            // LOG_SYSERR
        }
    }
    channel->set_index(-1);
}

bool EventLoop::isInLoopThread() const {
    return this == t_loopInThisThread;
}

void EventLoop::assertInLoopThread() {  //不在所属线程就抛异常
    if (!isInLoopThread()) {
        abort();
    }
}

void EventLoop::runInLoop(std::function<void()> cb) {
    if (isInLoopThread()) {
        cb();
    } else {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(std::function<void()> cb) {  //跨线程投递，操作的是当前 EventLoop 对象的 pendingFunctors_。
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingFunctors_.push_back(std::move(cb));
    }
    if (!isInLoopThread()) {
        wakeup();
    }
}

void EventLoop::doPendingFunctors() {  //执行回调函数
    std::vector<std::function<void()>> functors;
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        functors.swap(pendingFunctors_);  // 交换，锁内只交换指针 （O(1)），锁外执行回调（可能耗时）。减少锁竞争。
    }
    
    for (const auto& f : functors) {
        f();
    }
}

void EventLoop::wakeup() {
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        // LOG_ERROR
    }
}

void EventLoop::handleRead() {
    uint64_t one;
    ssize_t n = ::read(wakeupFd_, &one, sizeof(one));
    if (n != sizeof(one)) {
        // LOG_ERROR
    }
}

} // namespace rpc