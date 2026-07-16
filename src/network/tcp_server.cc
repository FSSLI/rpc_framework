// src/network/tcp_server.cc
#include "tcp_server.h"
#include "event_loop.h"
#include "acceptor.h"
#include "tcp_connection.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <cerrno>
#include <iostream>
#include "event_loop_thread_pool.h"

namespace rpc {

TcpServer::TcpServer(EventLoop* loop, const struct sockaddr_in& listenAddr)
    : loop_(loop),
      acceptor_(new Acceptor(loop, listenAddr)),
      started_(false),
      nextConnId_(1) {
    // Issue #7 fix: inet_ntop 替代过时且非线程安全的 inet_ntoa
    char ipStr[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &listenAddr.sin_addr, ipStr, sizeof(ipStr));
    name_ = ipStr;
    threadPool_ = std::make_shared<EventLoopThreadPool>(loop, name_);
    
    acceptor_->setNewConnectionCallback(
        std::bind(&TcpServer::newConnection, this, std::placeholders::_1, std::placeholders::_2));  //_1 和 _2 是占位符，表示"用回调时的参数"。
}

TcpServer::~TcpServer() {
    loop_->assertInLoopThread();
    // Issue #1 fix: 先取消 idle 定时器，防止 baseLoop 生命周期长于 TcpServer
    if (idleTimerId_ != 0) {
        loop_->cancelTimer(idleTimerId_);
    }
    for (auto& item : connections_) {
        TcpConnectionPtr conn(item.second);  //拷贝 shared_ptr，引用计数 +1
        item.second.reset(); // map 里的置空，引用计数 -1
        // 但 conn 还持有，引用计数 >= 1，不会析构
        conn->getLoop()->runInLoop(
            std::bind(&TcpConnection::connectDestroyed, conn));  //// 投递到 ioLoop 执行 connectDestroyed
    }
    // 循环结束，但 conn 的引用计数可能还 > 0（ioLoop 里 pending）
    // 等 ioLoop 执行完 connectDestroyed，引用计数变 0，才真正析构
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
            idleTimerId_ = loop_->runEvery(5.0, [this]() {  // Issue #1 fix: 保存 ID
                // 每 5 秒检查一次所有连接
                for (auto& item : connections_) {
                    item.second->checkIdleTimeout();
                }
            });
        }

        acceptor_->listen();  //启动监听
    }
}

void TcpServer::newConnection(int sockfd, const struct sockaddr_in& peerAddr) {  //监听到新连接后执行的函数
    loop_->assertInLoopThread();
    
    char buf[32];
    snprintf(buf, sizeof(buf), "-%s#%d", name_.c_str(), nextConnId_);
    ++nextConnId_;
    std::string connName = name_ + buf;
    
    // TODO: 从 threadPool_ 取 loop
    EventLoop* ioLoop = threadPool_->getNextLoop();
    
    // 3. 获取本地地址
    struct sockaddr_in localAddr;
    memset(&localAddr, 0, sizeof(localAddr));
    socklen_t addrlen = sizeof(localAddr);
    if (::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&localAddr), &addrlen) < 0) {
        // Issue #13: 打印警告，但连接仍可正常使用
        std::cerr << "TcpServer::newConnection getsockname failed, errno=" << errno << std::endl;
    }

    // 4. 创建 TcpConnection
    TcpConnectionPtr conn(new TcpConnection(ioLoop, connName, sockfd, localAddr, peerAddr));

    // 新增：设置 idle 超时
    if (idleTimeoutSeconds_ > 0) {
        conn->setIdleTimeout(idleTimeoutSeconds_);
    }

    // 6. 注册到连接表
    connections_[connName] = conn;
    
    // 5. 设置回调
    conn->setConnectionCallback(connectionCallback_);  // 连接建立/断开
    conn->setMessageCallback(messageCallback_);  // 收到消息
    conn->setWriteCompleteCallback(writeCompleteCallback_);  // 写完成
    conn->setCloseCallback(
        std::bind(&TcpServer::removeConnection, this, std::placeholders::_1));
    
    ioLoop->runInLoop(std::bind(&TcpConnection::connectEstablished, conn)); // 7. 在 ioLoop 线程执行连接建立
}

// 任何线程调用
void TcpServer::removeConnection(const TcpConnectionPtr& conn) {
    // 投递到 loop_ 线程（baseLoop）
    loop_->runInLoop(std::bind(&TcpServer::removeConnectionInLoop, this, conn));
}

// 在 loop_ 线程执行
void TcpServer::removeConnectionInLoop(const TcpConnectionPtr& conn) {
    loop_->assertInLoopThread();
    // map::erase 返回被移除的元素个数：
    // 找到并移除 → 返回 1
    // 没找到 → 返回 0
    size_t n = connections_.erase(conn->name());
    // EPOLLRDHUP/EPOLLERR 可能导致 handleRead→handleClose→closeCallback_ 和
    // Channel::handleEvent→closeCallback_ 双重调用。ignore duplicate removal.
    if (n == 0) return;
    
    EventLoop* ioLoop = conn->getLoop();  // 2. 在 ioLoop 线程执行 connectDestroyed
    // 用 shared_ptr 拷贝延长生命周期
    ioLoop->queueInLoop([conn]() {
        conn->connectDestroyed();
    });
}

} // namespace rpc