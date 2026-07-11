// src/network/event_loop_thread_pool.h
#ifndef EVENT_LOOP_THREAD_POOL_H
#define EVENT_LOOP_THREAD_POOL_H

#include <vector>
#include <memory>
#include <string>
#include <functional>

namespace rpc {

class EventLoop;
class EventLoopThread;

class EventLoopThreadPool {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;  //初始化时注册初始化回调函数，和EvEventLoopThreadPool 把回调传给每个 EventLoopThread

    EventLoopThreadPool(EventLoop* baseLoop, const std::string& name);  //主 EventLoop，也叫 accept loop。TcpServer 的构造函数里：
    ~EventLoopThreadPool();

    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    void setThreadNum(int numThreads) { numThreads_ = numThreads; }  //线程池线程数量
    void start(const ThreadInitCallback& cb = ThreadInitCallback()); //启动

    EventLoop* getNextLoop();  //获得下一个loop对象，负载均衡
    EventLoop* getLoopForHash(size_t hashCode);  //哈希均衡 一致性哈希场景用

    bool started() const { return started_; }
    const std::string& name() const { return name_; }  //线程池名称

private:
    EventLoop* baseLoop_;  //主循环
    std::string name_;  //名字
    bool started_;  //是否启动
    int numThreads_;  //线程数
    int next_;  //轮询索引，getNextLoop 里 (next_ + 1) % loops_.size()
    std::vector<std::unique_ptr<EventLoopThread>> threads_;  //线程池对象
    std::vector<EventLoop*> loops_;  // 子 EventLoop 指针数组，getNextLoop 从这里选
};

} // namespace rpc

#endif