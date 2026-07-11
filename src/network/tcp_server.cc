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
      name_(inet_ntoa(listenAddr.sin_addr)),  //inet_ntoa 把 IP 地址转字符串，如 "127.0.0.1"
      acceptor_(new Acceptor(loop, listenAddr)),
      threadPool_(std::make_shared<EventLoopThreadPool>(loop, name_)),  // ← 用 shared_ptr 或 unique_ptr
      started_(false),  
      nextConnId_(1)  {
    
    acceptor_->setNewConnectionCallback(
        std::bind(&TcpServer::newConnection, this, std::placeholders::_1, std::placeholders::_2));  //_1 和 _2 是占位符，表示"用回调时的参数"。
}

TcpServer::~TcpServer() {
    loop_->assertInLoopThread();
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
            loop_->runEvery(5.0, [this]() {
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
    if (::getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&localAddr), &addrlen) < 0) {  //getsockname 获取本端地址（bind 时设置的）
        // sockfd：输入，连接 fd
        // addr：输出，内核填充本地地址
        // addrlen：输入输出，输入是缓冲区大小，输出是实际地址大小
        // LOG_SYSERR << "getsockname";
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
    size_t n = connections_.erase(conn->name());  // 1. 从 map 移除
    (void)n;  // 消除未使用变量警告
    assert(n == 1);  // 断言：必须移除成功，且只移除 1 个
    
    EventLoop* ioLoop = conn->getLoop();  // 2. 在 ioLoop 线程执行 connectDestroyed
    // 用 shared_ptr 拷贝延长生命周期
    ioLoop->queueInLoop([conn]() {
        conn->connectDestroyed();
    });
}

} // namespace rpc