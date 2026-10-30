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

class TcpClient : public std::enable_shared_from_this<TcpClient> {
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

    // 所有权声明：一个 TcpClient 同一时刻只能被一个上层客户端（RpcAsyncClient）绑定。
    // 两者共享会互相覆盖 connection/message 回调，导致响应投递到错误的客户端。
    bool tryClaim(const void* owner) {
        const void* expected = nullptr;
        return owner_.compare_exchange_strong(expected, owner);
    }
    void releaseClaim(const void* owner) {
        const void* expected = owner;
        owner_.compare_exchange_strong(expected, nullptr);
    }
    bool isClaimed() const { return owner_.load(std::memory_order_acquire) != nullptr; }

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
    std::atomic<const void*> owner_{nullptr};  // 当前绑定的上层客户端（nullptr = 未占用）
};

} // namespace rpc

#endif