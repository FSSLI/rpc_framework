// src/network/channel.h
#ifndef CHANNEL_H
#define CHANNEL_H

#include <functional>
#include <sys/epoll.h>  // ← 加这行

namespace rpc {

class EventLoop;

class Channel {
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel();

    void handleEvent();  // 分发事件：根据 revents_ 的位标志，调用对应的回调函数

    //绑定回调函数
    void setReadCallback(EventCallback cb) { readCallback_ = std::move(cb); }
    void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }

    int fd() const { return fd_; }  //返回绑定的文件描述符
    int events() const { return events_; }  //返回感兴趣的事件
    void set_revents(int revt) { revents_ = revt; }  // 设置实际发生的事件（epoll_wait 返回后由 EventLoop 设置）
    //注册和取消事件
    void enableReading();  // 注册读事件：设置 events_ 标志，调用 update() 让 EventLoop 通过 epoll_ctl 注册
    void disableReading();
    void enableWriting();
    void disableWriting();
    void disableAll();

    //是否注册对应事件
    bool isWriting() const;  // 是否正在监听写事件：判断 events_ 中是否包含 EPOLLOUT
    bool isReading() const;
    bool isNoneEvent() const;  // ← 加这行

    //返回channel状态
    int index() const { return index_; }
    //设置状态
    void set_index(int idx) { index_ = idx; }

    EventLoop* ownerLoop() { return loop_; }  //返回所属eventLoop指针
    
    void remove();  // 从 epoll 移除

private:
    static const int kNoneEvent = 0;
    static const int kReadEvent = EPOLLIN | EPOLLPRI;
    static const int kWriteEvent = EPOLLOUT;

    EventLoop* loop_;  //所属eventLoop
    const int fd_;  //绑定的文件描述符
    int events_;  //注册的感兴趣的事件
    int revents_;  //实际发生事件
    int index_;  //channel状态

// index	含义
// -1	    未注册（初始/已移除）
// 1	    已注册到 epoll
// 2	    已标记删除（但可能还在 epoll 里）

    EventCallback readCallback_;  //注册读回调函数
    EventCallback writeCallback_;  //注册写回调函数
    EventCallback errorCallback_;  //注册错误回调函数
    EventCallback closeCallback_;  //注册关闭回调函数

    void update();  // // 更新事件注册：调用 loop_->updateChannel(this)，由 EventLoop 统一操作 epoll_ctl 
};

} // namespace rpc

#endif