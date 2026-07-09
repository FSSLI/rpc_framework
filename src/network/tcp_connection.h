// src/network/tcp_connection.h
#ifndef TCP_CONNECTION_H
#define TCP_CONNECTION_H

#include <memory>
#include <functional>
#include <atomic>
#include "buffer.h"
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

    void connectEstablished();
    void connectDestroyed();

    void send(const std::string& message);
    void send(Buffer* message);
    void shutdown();

    void setContext(void* context) { context_ = context; }
    void* getContext() const { return context_; }

private:
    enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };

    void setState(StateE s);

    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

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

    Buffer inputBuffer_;
    Buffer outputBuffer_;

    void* context_;  // 用于绑定 RPC 上下文
};

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

} // namespace rpc

#endif