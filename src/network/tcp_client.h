// src/network/tcp_client.h
#ifndef TCP_CLIENT_H
#define TCP_CLIENT_H

#include <functional>
#include <memory>
#include <mutex>
#include <netinet/in.h>  // sockaddr_in
#include "buffer.h"  // ← 加这行

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

    std::shared_ptr<TcpConnection> connection() const;

private:
    void newConnection(int sockfd);
    void removeConnection(const std::shared_ptr<TcpConnection>& conn);

    EventLoop* loop_;
    std::unique_ptr<Connector> connector_;
    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    std::mutex mutex_;
    std::shared_ptr<TcpConnection> connection_;
};

} // namespace rpc

#endif