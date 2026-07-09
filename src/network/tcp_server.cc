// src/network/tcp_server.cc
#include "tcp_server.h"
#include "event_loop.h"
#include "acceptor.h"
#include "tcp_connection.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>  // ← 加这行，memset
#include "event_loop_thread_pool.h"

namespace rpc {

TcpServer::TcpServer(EventLoop* loop, const struct sockaddr_in& listenAddr)
    : loop_(loop),
      name_(inet_ntoa(listenAddr.sin_addr)),
      acceptor_(new Acceptor(loop, listenAddr)),
      threadPool_(std::make_shared<EventLoopThreadPool>(loop, name_)),  // ← 用 shared_ptr 或 unique_ptr
      started_(false),
      nextConnId_(1)  {
    
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
    threadPool_->setThreadNum(numThreads);
}

void TcpServer::start() {
    if (!started_) {
        started_ = true;
        threadPool_->start();

        // 新增：启动 idle 检测定时器
        if (idleTimeoutSeconds_ > 0) {
            loop_->runEvery(5.0, [this]() {
                // 每 5 秒检查一次所有连接
                for (auto& item : connections_) {
                    item.second->checkIdleTimeout();
                }
            });
        }

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
    EventLoop* ioLoop = threadPool_->getNextLoop();
    
    struct sockaddr_in localAddr;
    memset(&localAddr, 0, sizeof(localAddr));
    socklen_t addrlen = sizeof(localAddr);
    if (::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&localAddr), &addrlen) < 0) {
        // LOG_SYSERR << "getsockname";
    }
    
    TcpConnectionPtr conn(new TcpConnection(ioLoop, connName, sockfd, localAddr, peerAddr));

    // 新增：设置 idle 超时
    if (idleTimeoutSeconds_ > 0) {
        conn->setIdleTimeout(idleTimeoutSeconds_);
    }

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
    // 用 shared_ptr 拷贝延长生命周期
    ioLoop->queueInLoop([conn]() {
        conn->connectDestroyed();
    });
}

} // namespace rpc