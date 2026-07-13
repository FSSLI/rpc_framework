// src/network/tcp_connection.h
#ifndef TCP_CONNECTION_H
#define TCP_CONNECTION_H

#include <memory>
#include <functional>
#include <atomic>
#include <chrono>  // ← 新增
#include "network/buffer.h"
#include <netinet/in.h>

namespace rpc {

class EventLoop;
class Channel;
class Buffer;
class Socket;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    using ConnectionCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    using MessageCallback = std::function<void(const std::shared_ptr<TcpConnection>&, Buffer*, int64_t)>;
    using WriteCompleteCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    using CloseCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    // 新增：高水位回调类型
    using HighWaterMarkCallback = std::function<void(const std::shared_ptr<TcpConnection>&, size_t)>;

    TcpConnection(EventLoop* loop,
                  const std::string& name,
                  int sockfd,
                  const struct sockaddr_in& localAddr,
                  const struct sockaddr_in& peerAddr);
    ~TcpConnection();

    EventLoop* getLoop() const { return loop_; }
    const std::string& name() const { return name_; }

    void setConnectionCallback(const ConnectionCallback& cb) { connectionCallback_ = cb; }
    void setMessageCallback(const MessageCallback& cb) { messageCallback_ = cb; }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) { writeCompleteCallback_ = cb; }
    void setCloseCallback(const CloseCallback& cb) { closeCallback_ = cb; }

    void setHighWaterMarkCallback(const HighWaterMarkCallback& cb, size_t highWaterMark) {
        highWaterMarkCallback_ = cb;
        highWaterMark_ = highWaterMark;
    }

    void connectEstablished();  // 连接建立：设置状态、注册 Channel、回调
    void connectDestroyed();  // 连接销毁：注销 Channel、清理资源

    void send(const std::string& message);  // 跨线程安全发送
    void send(Buffer* message); 
    void shutdown();  // 关闭写端

    void setContext(void* context) { context_ = context; }
    void* getContext() const { return context_; }

    // 新增：idle 超时检测
    void setIdleTimeout(int seconds);
    void updateActiveTime();
    bool checkIdleTimeout();

    // 新增：判断连接是否已建立
    bool connected() const { return state_ == kConnected; }

    void forceClose();  // 强制关闭

private:
    enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };  //StateE 状态机

    void setState(StateE s);

    void handleRead();   // Channel 可读回调：读数据到 inputBuffer_
    void handleWrite();  // Channel 可写回调：写 outputBuffer_ 到 fd
    void handleClose();  // Channel 关闭回调：连接断开
    void handleError();  // Channel 错误回调：错误处理

    void sendInLoop(const std::string& message);
    void shutdownInLoop();

    EventLoop* loop_;
    std::string name_;
    std::atomic<StateE> state_;

    std::unique_ptr<Socket> socket_;
    std::unique_ptr<Channel> channel_;

    struct sockaddr_in localAddr_;
    struct sockaddr_in peerAddr_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    CloseCallback closeCallback_;

    Buffer inputBuffer_;  // 读缓冲区：从 fd 读入，业务逻辑消费
    Buffer outputBuffer_;  // 写缓冲区：业务逻辑写入，fd 可写时发送

    void* context_;  // 用户上下文，绑定任意数据
    
    // 新增：idle 超时（atomic int64 防止 ioLoop/baseLoop 跨线程数据竞争）
    int idleTimeoutSeconds_ = 0;
    std::atomic<int64_t> lastActiveTimeMs_{0};

    size_t highWaterMark_ = 0;                      // 高水位阈值，0 表示不检测
    HighWaterMarkCallback highWaterMarkCallback_;   // 超过阈值时的回调
};

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

} // namespace rpc

#endif