// src/network/tcp_client.cc
#include "tcp_client.h"
#include "connector.h"
#include "tcp_connection.h"
#include "event_loop.h"
#include "socket.h"
#include <iostream>

namespace rpc {

TcpClient::TcpClient(EventLoop* loop, const struct sockaddr_in& serverAddr)
    : loop_(loop),
      connector_(std::make_shared<Connector>(loop, serverAddr)) {

    connector_->setNewConnectionCallback(
        std::bind(&TcpClient::newConnection, this, std::placeholders::_1));
}

TcpClient::~TcpClient() {
    // ~TcpClient() must run in the loop thread because:
    // - Connector::~Connector() calls assertInLoopThread()
    // - removeConnection callback may fire during connection cleanup
    // The caller (ConnectionPool) must dispatch shared_ptr<TcpClient> to
    // the loop thread before releasing it, or call ~TcpClient from loop.
    // If destructor is called from non-loop thread, post to loop:
    if (connector_ && loop_ && !loop_->isInLoopThread()) {
        auto c = std::move(connector_);
        std::shared_ptr<TcpConnection> conn;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            conn = std::move(connection_);
        }
        loop_->runInLoop([c = std::move(c), conn = std::move(conn)]() {
            if (conn) conn->connectDestroyed();
            // ~Connector() runs when c goes out of scope (in loop thread)
        });
        return;
    }
    // In loop thread: safe to destroy directly
    if (connector_) connector_->stop();

    std::shared_ptr<TcpConnection> conn;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        conn = connection_;
    }
    if (conn) {
        auto ioLoop = conn->getLoop();
        ioLoop->runInLoop([conn]() { conn->connectDestroyed(); });
    }
}

void TcpClient::connect() {
    connector_->start();
}

void TcpClient::disconnect() {
    auto conn = connection();  // 线程安全获取 shared_ptr 副本
    if (conn) {
        conn->shutdown();
    }
}

void TcpClient::stop() {
    connector_->stop();
}

void TcpClient::disconnectPermanently() {
    disconnecting_ = true;
    connector_->stop();  // 先停止重连
    auto conn = connection();
    if (conn) {
        conn->shutdown();
    }
}

void TcpClient::clearCallbacks() {
    setConnectionCallback({});
    setMessageCallback({});
    setWriteCompleteCallback({});
    auto conn = connection();
    if (conn) {
        conn->setConnectionCallback({});
        conn->setMessageCallback({});
        conn->setWriteCompleteCallback({});
    }
}

void TcpClient::ensureConnected() {
    std::weak_ptr<TcpClient> weakSelf = weak_from_this();
    loop_->runInLoop([weakSelf]() {
        auto self = weakSelf.lock();
        if (!self) return;

        auto conn = self->connection();
        if (conn && conn->connected()) return;          // 已经连着
        if (self->connector_ && self->connector_->isConnecting()) return;  // 正在连，别重复
        if (self->connector_) self->connector_->restart();
    });
}

std::shared_ptr<TcpConnection> TcpClient::connection() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connection_;
}

void TcpClient::newConnection(int sockfd) {
    struct sockaddr_in localAddr = Socket::getLocalAddr(sockfd);
    struct sockaddr_in peerAddr = Socket::getPeerAddr(sockfd);

    TcpConnectionPtr conn(new TcpConnection(loop_,
                                              "TcpClient-" + std::to_string(sockfd),
                                              sockfd,
                                              localAddr,
                                              peerAddr));

    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);
    conn->setWriteCompleteCallback(writeCompleteCallback_);
    // 用 weak_ptr 绑定：TcpClient 可能在主线程析构，而 IO 线程之后仍会处理该连接的关闭事件。
    // 裸 this 会导致 use-after-free（ASan 实证）。
    std::weak_ptr<TcpClient> weakSelf = weak_from_this();
    conn->setCloseCallback([weakSelf](const TcpConnectionPtr& c) {
        if (auto self = weakSelf.lock()) self->removeConnection(c);
    });

    {
        std::lock_guard<std::mutex> lock(mutex_);
        connection_ = conn;
    }

    conn->connectEstablished();
}

void TcpClient::removeConnection(const TcpConnectionPtr& conn) {
    loop_->assertInLoopThread();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 连接可能在 shutdown 期间已被新连接替换（auto-reconnect），忽略不匹配的回调
        if (connection_ != conn) return;
        connection_.reset();
    }

    // Issue #2 fix: 检查 disconnecting_，防止 stop() 后被 restart() 覆盖
    if (retryOnDisconnect_ && !disconnecting_.load()) {
        connector_->restart();
    }

    if (connectionCallback_) {
        connectionCallback_(conn);
    }
}

} // namespace rpc