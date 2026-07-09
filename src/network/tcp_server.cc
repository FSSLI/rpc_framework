// src/network/tcp_server.cc
#include "tcp_server.h"
#include "event_loop.h"
#include "acceptor.h"
#include "tcp_connection.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>  // ← 加这行，memset

namespace rpc {

TcpServer::TcpServer(EventLoop* loop, const struct sockaddr_in& listenAddr)
    : loop_(loop),
      name_(inet_ntoa(listenAddr.sin_addr)),
      acceptor_(new Acceptor(loop, listenAddr)),
      threadPool_(nullptr),
      started_(false),
      nextConnId_(1) {
    
    acceptor_->setNewConnectionCallback(
        std::bind(&TcpServer::newConnection, this, std::placeholders::_1, std::placeholders::_2));
}

TcpServer::~TcpServer() {
    loop_->assertInLoopThread();
    for (auto& item : connections_) {
        TcpConnectionPtr conn(item.second);
        item.second.reset();
        conn->getLoop()->runInLoop(
            std::bind(&TcpConnection::connectDestroyed, conn));
    }
}

void TcpServer::setThreadNum(int numThreads) {
    // TODO: 创建 EventLoopThreadPool
    (void)numThreads;
}

void TcpServer::start() {
    if (!started_) {
        started_ = true;
        acceptor_->listen();
    }
}

void TcpServer::newConnection(int sockfd, const struct sockaddr_in& peerAddr) {
    loop_->assertInLoopThread();
    
    char buf[32];
    snprintf(buf, sizeof(buf), "-%s#%d", name_.c_str(), nextConnId_);
    ++nextConnId_;
    std::string connName = name_ + buf;
    
    // TODO: 从 threadPool_ 取 loop
    EventLoop* ioLoop = loop_;
    
    struct sockaddr_in localAddr;
    memset(&localAddr, 0, sizeof(localAddr));
    socklen_t addrlen = sizeof(localAddr);
    if (::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&localAddr), &addrlen) < 0) {
        // LOG_SYSERR << "getsockname";
    }
    
    TcpConnectionPtr conn(new TcpConnection(ioLoop, connName, sockfd, localAddr, peerAddr));
    connections_[connName] = conn;
    
    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);
    conn->setWriteCompleteCallback(writeCompleteCallback_);
    conn->setCloseCallback(
        std::bind(&TcpServer::removeConnection, this, std::placeholders::_1));
    
    ioLoop->runInLoop(std::bind(&TcpConnection::connectEstablished, conn));
}

void TcpServer::removeConnection(const TcpConnectionPtr& conn) {
    loop_->runInLoop(std::bind(&TcpServer::removeConnectionInLoop, this, conn));
}

void TcpServer::removeConnectionInLoop(const TcpConnectionPtr& conn) {
    loop_->assertInLoopThread();
    size_t n = connections_.erase(conn->name());
    (void)n;
    assert(n == 1);
    
    EventLoop* ioLoop = conn->getLoop();
    ioLoop->queueInLoop(std::bind(&TcpConnection::connectDestroyed, conn));
}

} // namespace rpc