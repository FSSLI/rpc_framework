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
    // LOG_DEBUG << "Connector::~Connector";
    assert(state_ == State::kDisconnected);
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
        // LOG_SYSFATAL << "Connector::connect socket";
        std::cerr << "Connector::connect socket error" << std::endl;
        return;
    }

    int ret = ::connect(sockfd, reinterpret_cast<const struct sockaddr*>(&serverAddr_),
                        static_cast<socklen_t>(sizeof(serverAddr_)));
    int savedErrno = (ret == 0) ? 0 : errno;

    switch (savedErrno) {
        case 0:
        case EINPROGRESS:       // 非阻塞 connect 正常返回，正在连接中
        case EINTR:
        case EISCONN:           // 已经连接上了（重复 connect）
            connecting(sockfd);
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
        case EALREADY:
        case EBADF:
        case EFAULT:
        case ENOTSOCK:
            // LOG_SYSERR << "Connector::connect unexpected error " << savedErrno;
            std::cerr << "Connector::connect unexpected error " << savedErrno << std::endl;
            ::close(sockfd);
            break;

        default:
            // LOG_SYSERR << "Connector::connect unknown error " << savedErrno;
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
    // LOG_TRACE << "Connector::handleWrite state=" << static_cast<int>(state_.load());
    
    if (state_ == State::kConnecting) {
        int sockfd = removeAndResetChannel();
        int err = Socket::getSocketError(sockfd);
        
        if (err) {  // connect 失败
            // LOG_WARN << "Connector::handleWrite - SO_ERROR = " << err;
            std::cerr << "Connector::handleWrite - SO_ERROR = " << err << std::endl;
            retry(sockfd);
        } else {    // connect 成功
            // LOG_TRACE << "Connector::handleWrite - connected";
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
    // LOG_ERROR << "Connector::handleError state=" << static_cast<int>(state_.load());
    
    if (state_ == State::kConnecting) {
        int sockfd = removeAndResetChannel();
        int err = Socket::getSocketError(sockfd);
        // LOG_WARN << "Connector::handleError - SO_ERROR = " << err;
        std::cerr << "Connector::handleError - SO_ERROR = " << err << std::endl;
        retry(sockfd);
    }
}

// ============================================================================
// 指数退避重连
// ============================================================================
// ============================================================================
// 重连逻辑
// ============================================================================
// TODO(Week 2): 重连测试待补。当前生产环境由 TcpClient 持有 Connector，
// 生命周期一致，不会提前析构。测试代码需用 shared_ptr 管理 Connector
// 或实现 EventLoop::cancelTimer 接口。
// connector.cc 中 retry 函数改为：
void Connector::retry(int sockfd) {
    ::close(sockfd);
    setState(State::kDisconnected);
    
    if (connect_) {
        std::cout << "Connector::retry - Retry connecting in " << retryDelayMs_ << " ms" << std::endl;
        
        // 不捕获 this，用 std::bind + 依赖调用者保证生命周期
        // 或者干脆不延迟，直接 startInLoop
        // loop_->runAfter(retryDelayMs_ / 1000.0,
        //                 std::bind(&Connector::startInLoop, this));
        
        // 更安全的做法：用 EventLoop 的 runAfter，但回调里检查 state
        loop_->runAfter(retryDelayMs_ / 1000.0, [this]() {
            if (connect_) {  // 检查是否还被要求连接
                startInLoop();
            }
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