// src/network/connector.cc
#include "connector.h"
#include "event_loop.h"
#include "channel.h"
#include "socket.h"
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <iostream>

namespace rpc {

Connector::Connector(EventLoop* loop, const struct sockaddr_in& serverAddr)
    : loop_(loop),
      serverAddr_(serverAddr),
      state_(State::kDisconnected),
      connect_(false),
      retryDelayMs_(kInitRetryDelayMs) {
}

Connector::~Connector() {
    // 安全清理：如果还在连接中，停止并关闭 fd
    if (state_ == State::kConnecting && channel_) {
        int sockfd = removeAndResetChannel();
        ::close(sockfd);
    }
    // 如果 state_ 是 kConnected，说明 fd 已经交给上层，无需处理
    // weak_ptr 保证 pending 定时器回调安全跳过
}

// ============================================================================
// 公共接口
// ============================================================================

void Connector::start() {
    connect_ = true;
    loop_->runInLoop(std::bind(&Connector::startInLoop, this));
}

void Connector::restart() {
    loop_->assertInLoopThread();
    setState(State::kDisconnected);
    retryDelayMs_ = kInitRetryDelayMs;
    connect_ = true;
    startInLoop();
}

void Connector::stop() {
    connect_ = false;
    loop_->queueInLoop(std::bind(&Connector::stopInLoop, this));
}

// ============================================================================
// 内部方法（必须在 loop 线程执行）
// ============================================================================

void Connector::startInLoop() {
    loop_->assertInLoopThread();
    assert(state_ == State::kDisconnected);
    if (connect_) {
        connect();
    }
}

void Connector::stopInLoop() {
    loop_->assertInLoopThread();
    if (state_ == State::kConnecting) {
        setState(State::kDisconnected);
        int sockfd = removeAndResetChannel();
        ::close(sockfd);
    }
}

// ============================================================================
// 核心：非阻塞 connect
// ============================================================================

void Connector::connect() {
    int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (sockfd < 0) {
        std::cerr << "Connector::connect socket error: " << errno << std::endl;
        // Issue #7 fix: socket 创建失败也走 retry，而非静默失败
        if (connect_) {
            retry(-1);  // retry 会 close(-1) 无害，然后按指数退避重连
        }
        return;
    }

    int ret = ::connect(sockfd, reinterpret_cast<const struct sockaddr*>(&serverAddr_),
                        static_cast<socklen_t>(sizeof(serverAddr_)));
    int savedErrno = (ret == 0) ? 0 : errno;

    switch (savedErrno) {
        case 0:
        case EINPROGRESS:       // 非阻塞 connect 正常返回，正在连接中
        case EALREADY:          // 重复调用非阻塞 connect 的正常状态
        case EISCONN:           // 已经连接上了（重复 connect）
            connecting(sockfd);
            break;

        // 信号中断，安全重试
        case EINTR:
            retry(sockfd);
            break;

        // 临时错误，可以重试
        case EAGAIN:
        case EADDRINUSE:
        case EADDRNOTAVAIL:
        case ECONNREFUSED:
        case ENETUNREACH:
            retry(sockfd);
            break;

        // 致命错误，不重试
        case EACCES:
        case EPERM:
        case EAFNOSUPPORT:
        case EBADF:
        case EFAULT:
        case ENOTSOCK:
            std::cerr << "Connector::connect unexpected error " << savedErrno << std::endl;
            ::close(sockfd);
            break;

        default:
            std::cerr << "Connector::connect unknown error " << savedErrno << std::endl;
            ::close(sockfd);
            break;
    }
}

// ============================================================================
// 注册可写事件，等待 connect 结果
// ============================================================================

void Connector::connecting(int sockfd) {
    setState(State::kConnecting);
    channel_.reset(new Channel(loop_, sockfd));
    channel_->setWriteCallback(std::bind(&Connector::handleWrite, this));
    channel_->setErrorCallback(std::bind(&Connector::handleError, this));
    channel_->enableWriting();
}

// ============================================================================
// connect 完成回调（可写事件触发）
// ============================================================================

void Connector::handleWrite() {
    if (state_ == State::kConnecting) {
        int sockfd = removeAndResetChannel();
        int err = Socket::getSocketError(sockfd);

        if (err) {  // connect 失败
            std::cerr << "Connector::handleWrite - SO_ERROR = " << err << std::endl;
            retry(sockfd);
        } else {    // connect 成功
            setState(State::kConnected);
            if (connect_) {
                newConnectionCallback_(sockfd);
            } else {
                ::close(sockfd);
            }
        }
    }
}

// ============================================================================
// connect 出错回调
// ============================================================================

void Connector::handleError() {
    if (state_ == State::kConnecting) {
        int sockfd = removeAndResetChannel();
        int err = Socket::getSocketError(sockfd);
        std::cerr << "Connector::handleError - SO_ERROR = " << err << std::endl;
        retry(sockfd);
    }
}

// ============================================================================
// 指数退避重连
// ============================================================================

void Connector::retry(int sockfd) {
    ::close(sockfd);
    setState(State::kDisconnected);

    if (connect_) {
        std::cout << "Connector::retry - Retry connecting in " << retryDelayMs_ << " ms" << std::endl;

        // 使用 weak_ptr 防止 Connector 析构后定时器回调访问悬空指针
        std::weak_ptr<Connector> weakThis = shared_from_this();
        loop_->runAfter(retryDelayMs_ / 1000.0, [weakThis]() {
            auto ptr = weakThis.lock();
            if (!ptr) return;              // Connector 已析构
            // Issue #1 fix: 检查 connect_ 和 state，防止 stop() 后意外重连
            if (!ptr->connect_ || ptr->state_ != State::kDisconnected) return;
            ptr->startInLoop();
        });

        retryDelayMs_ = std::min(retryDelayMs_ * 2, kMaxRetryDelayMs);
    }
}

// ============================================================================
// 工具方法
// ============================================================================

int Connector::removeAndResetChannel() {
    channel_->disableAll();
    channel_->remove();
    int sockfd = channel_->fd();
    channel_.reset();
    return sockfd;
}

void Connector::setState(State s) {
    state_ = s;
}

// ============================================================================
// 静态常量定义（C++14 类内初始化仍需类外定义）
// ============================================================================
const int Connector::kMaxRetryDelayMs;
const int Connector::kInitRetryDelayMs;

} // namespace rpc