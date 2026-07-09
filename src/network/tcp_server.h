// tcp_server.h
#ifndef TCP_SERVER_H
#define TCP_SERVER_H

#include <functional>
#include <memory>
#include <vector>
#include <map>
#include <string>
#include <netinet/in.h>
#include "buffer.h"


namespace rpc {

class EventLoop;
class Acceptor;
class TcpConnection;
class EventLoopThreadPool;

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;  // ← 加这行

class TcpServer {
public:
    using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
    using MessageCallback = std::function<void(const TcpConnectionPtr&, Buffer*, int64_t)>;
    using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;

    TcpServer(EventLoop* loop, const struct sockaddr_in& listenAddr);
    ~TcpServer();

    void setThreadNum(int numThreads);
    void setConnectionCallback(const ConnectionCallback& cb) { connectionCallback_ = cb; }
    void setMessageCallback(const MessageCallback& cb) { messageCallback_ = cb; }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) { writeCompleteCallback_ = cb; }

    void start();

private:
    void newConnection(int sockfd, const struct sockaddr_in& peerAddr);
    void removeConnection(const TcpConnectionPtr& conn);
    void removeConnectionInLoop(const TcpConnectionPtr& conn);  // ← 加这行

    EventLoop* loop_;
    const std::string name_;
    std::unique_ptr<Acceptor> acceptor_;
    std::shared_ptr<EventLoopThreadPool> threadPool_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    bool started_;
    int nextConnId_;
    std::map<std::string, TcpConnectionPtr> connections_;
};

} // namespace rpc

#endif