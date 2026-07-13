// src/network/tcp_client.h
#ifndef TCP_CLIENT_H
#define TCP_CLIENT_H

#include <functional>
#include <memory>
#include <mutex>
#include <atomic>
#include <netinet/in.h>  // sockaddr_in
#include "buffer.h"

namespace rpc {

class EventLoop;
class Connector;
class TcpConnection;

class TcpClient {
public:
    using ConnectionCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    using MessageCallback = std::function<void(const std::shared_ptr<TcpConnection>&, Buffer*, int64_t)>;
    using WriteCompleteCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;

    TcpClient(EventLoop* loop, const struct sockaddr_in& serverAddr);
    ~TcpClient();

    void connect();
    void disconnect();
    void stop();

    void setConnectionCallback(const ConnectionCallback& cb) { connectionCallback_ = cb; }
    void setMessageCallback(const MessageCallback& cb) { messageCallback_ = cb; }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) { writeCompleteCallback_ = cb; }

    // 控制断开后是否自动重连（默认 true，连接池场景设为 false）
    void setRetryOnDisconnect(bool on) { retryOnDisconnect_ = on; }

    // 获取底层 EventLoop 指针（连接池注入用）
    EventLoop* getLoop() const { return loop_; }

    // 永久断开：停止重连并关闭连接（连接池回收用）
    void disconnectPermanently();

    std::shared_ptr<TcpConnection> connection() const;

private:
    void newConnection(int sockfd);
    void removeConnection(const std::shared_ptr<TcpConnection>& conn);

    EventLoop* loop_;
    std::shared_ptr<Connector> connector_;  // shared_ptr 支持 weak_ptr，防止定时器回调 use-after-free
    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    mutable std::mutex mutex_;
    std::shared_ptr<TcpConnection> connection_;
    bool retryOnDisconnect_ = true;  // Issue #1 fix: 断开后是否自动重连
    std::atomic<bool> disconnecting_{false};  // Issue #2 fix: 防止 disconnect 后自动重连
};

} // namespace rpc

#endif