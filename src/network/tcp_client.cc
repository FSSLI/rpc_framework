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
      connector_(new Connector(loop, serverAddr)) {
    
    connector_->setNewConnectionCallback(
        std::bind(&TcpClient::newConnection, this, std::placeholders::_1));
}

TcpClient::~TcpClient() {
    // LOG_DEBUG << "TcpClient::~TcpClient";
    if (connection_) {
        connection_->connectDestroyed();
    }
}

void TcpClient::connect() {
    connector_->start();
}

void TcpClient::disconnect() {
    if (connection_) {
        connection_->shutdown();
    }
}

void TcpClient::stop() {
    connector_->stop();
}

std::shared_ptr<TcpConnection> TcpClient::connection() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connection_;
}

// ============================================================================
// Connector 成功回调：创建 TcpConnection
// ============================================================================

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
    conn->setCloseCallback(std::bind(&TcpClient::removeConnection, this, std::placeholders::_1));
    
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connection_ = conn;
    }
    
    conn->connectEstablished();
}

// ============================================================================
// TcpConnection 关闭回调：移除连接
// ============================================================================

void TcpClient::removeConnection(const TcpConnectionPtr& conn) {
    loop_->assertInLoopThread();
    
    {
        std::lock_guard<std::mutex> lock(mutex_);
        assert(connection_ == conn);
        connection_.reset();
    }
    
    // 连接断开，触发重连
    connector_->restart();
    
    // 通知用户连接断开
    if (connectionCallback_) {
        connectionCallback_(conn);
    }
}

} // namespace rpc