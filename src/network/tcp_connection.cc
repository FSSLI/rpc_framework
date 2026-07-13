// src/network/tcp_connection.cc
#include "tcp_connection.h"
#include "event_loop.h"
#include "channel.h"
#include "buffer.h"
#include "socket.h"
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <errno.h>
#include <iostream>

namespace rpc {

TcpConnection::TcpConnection(EventLoop* loop,
                             const std::string& name,
                             int sockfd,
                             const struct sockaddr_in& localAddr,
                             const struct sockaddr_in& peerAddr)
    : loop_(loop),
      name_(name),
      state_(kConnecting),
      socket_(new Socket(sockfd)),
      channel_(new Channel(loop, sockfd)),
      localAddr_(localAddr),
      peerAddr_(peerAddr),
      context_(nullptr),
      idleTimeoutSeconds_(0) {
    updateActiveTime();  // 初始化 lastActiveTimeMs_

    channel_->setReadCallback(std::bind(&TcpConnection::handleRead, this));
    channel_->setWriteCallback(std::bind(&TcpConnection::handleWrite, this));
    channel_->setCloseCallback(std::bind(&TcpConnection::handleClose, this));
    channel_->setErrorCallback(std::bind(&TcpConnection::handleError, this));

    socket_->setKeepAlive(true);
}

TcpConnection::~TcpConnection() {
    // LOG_DEBUG
}

void TcpConnection::setIdleTimeout(int seconds) {
    idleTimeoutSeconds_ = seconds;
    updateActiveTime();
}

// Issue #2 fix: 改用 atomic<int64_t> 毫秒时间戳，消除多线程读写 time_point 的数据竞争
void TcpConnection::updateActiveTime() {
    auto now = std::chrono::steady_clock::now();
    lastActiveTimeMs_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count(),
        std::memory_order_relaxed);
}

bool TcpConnection::checkIdleTimeout() {
    if (idleTimeoutSeconds_ <= 0) return false;

    auto now = std::chrono::steady_clock::now();
    auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    auto lastMs = lastActiveTimeMs_.load(std::memory_order_relaxed);
    auto elapsed = (nowMs - lastMs) / 1000;

    if (elapsed >= idleTimeoutSeconds_) {
        forceClose();
        return true;
    }
    return false;
}

void TcpConnection::connectEstablished() {
    loop_->assertInLoopThread();
    assert(state_ == kConnecting);
    setState(kConnected);
    channel_->enableReading();
    std::cout << "connectEstablished: " << name_ << " fd=" << channel_->fd() << std::endl;

    if (connectionCallback_) {
        connectionCallback_(shared_from_this());
    }
}

void TcpConnection::connectDestroyed() {
    loop_->assertInLoopThread();
    if (state_ == kConnected) {
        setState(kDisconnected);
        channel_->disableAll();

        if (connectionCallback_) {
            connectionCallback_(shared_from_this());
        }
    }
    channel_->remove();
}

// ============================================================================
// send 优化：用 shared_ptr<string> 避免跨线程拷贝
// ============================================================================

void TcpConnection::send(const std::string& message) {
    if (state_ != kConnected) {
        return;
    }

    if (loop_->isInLoopThread()) {
        sendInLoop(message);
    } else {
        // Issue #1 fix: shared_from_this() 替代裸 this，防止 lambda 执行时对象已析构
        auto msgPtr = std::make_shared<std::string>(message);
        loop_->runInLoop([self = shared_from_this(), msgPtr]() {
            if (self->state_ == kConnected) {
                self->sendInLoop(*msgPtr);
            }
        });
    }
}

void TcpConnection::send(Buffer* buf) {
    if (state_ != kConnected) {
        return;
    }

    if (loop_->isInLoopThread()) {
        sendInLoop(buf->retrieveAllAsString());
    } else {
        // 跨线程：拷贝到 shared_ptr 后清空 buffer（muduo 风格：调用者已转移所有权）
        // Trade-off: 若 lambda 因连接断开未执行，数据在 msgPtr 中但不会被发送。
        // 调用者应在发送前检查连接状态。
        auto msgPtr = std::make_shared<std::string>(buf->peek(), buf->readableBytes());
        buf->retrieveAll();
        loop_->runInLoop([self = shared_from_this(), msgPtr]() {
            if (self->state_ == kConnected) {
                self->sendInLoop(*msgPtr);
            }
        });
    }
}

void TcpConnection::shutdown() {
    if (state_ == kConnected) {
        setState(kDisconnecting);
        loop_->runInLoop([self = shared_from_this()]() {
            self->shutdownInLoop();
        });
    }
}

void TcpConnection::sendInLoop(const std::string& message) {
    loop_->assertInLoopThread();
    ssize_t nwrote = 0;
    size_t remaining = message.size();
    bool faultError = false;

    if (state_ == kDisconnected) {
        return;
    }

    // 如果 outputBuffer_ 为空，直接写
    if (!channel_->isWriting() && outputBuffer_.readableBytes() == 0) {
        nwrote = ::write(channel_->fd(), message.data(), message.size());
        if (nwrote >= 0) {
            remaining = message.size() - nwrote;
            if (remaining == 0 && writeCompleteCallback_) {
                loop_->queueInLoop(std::bind(writeCompleteCallback_, shared_from_this()));
            }
        } else {
            nwrote = 0;
            // Issue #8 fix: 可移植的非阻塞写判断
#if EAGAIN != EWOULDBLOCK
            if (errno != EAGAIN && errno != EWOULDBLOCK)
#else
            if (errno != EAGAIN)
#endif
            {
                if (errno == EPIPE || errno == ECONNRESET) {
                    faultError = true;
                }
            }
        }
    }

    // Issue #7 fix: 致命错误时主动关闭连接，避免僵尸连接
    if (faultError) {
        handleError();
        return;
    }

    if (remaining > 0) {
        outputBuffer_.append(message.data() + nwrote, remaining);

        // FIX: 高水位检测
        if (highWaterMark_ > 0 && outputBuffer_.readableBytes() > highWaterMark_) {
            if (highWaterMarkCallback_) {
                highWaterMarkCallback_(shared_from_this(), outputBuffer_.readableBytes());
            }
        }

        if (!channel_->isWriting()) {
            channel_->enableWriting();
        }
    }
}

void TcpConnection::shutdownInLoop() {
    loop_->assertInLoopThread();
    if (!channel_->isWriting()) {
        socket_->shutdownWrite();
    }
}

void TcpConnection::handleRead() {
    loop_->assertInLoopThread();
    updateActiveTime();

    std::cout << "handleRead: " << name_ << std::endl;

    int savedErrno = 0;
    int64_t receiveTime = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    ssize_t n = inputBuffer_.readFd(channel_->fd(), &savedErrno);

    std::cout << "readFd n=" << n << " errno=" << savedErrno << std::endl;

    if (n > 0) {
        if (messageCallback_) {
            messageCallback_(shared_from_this(), &inputBuffer_, receiveTime);
        } else {
            // Issue #15 fix: 无人消费时丢弃数据，防止 inputBuffer_ 无限增长
            inputBuffer_.retrieveAll();
        }
    } else if (n == 0) {
        handleClose();
    } else {
        errno = savedErrno;
        handleError();
    }
}

void TcpConnection::handleWrite() {
    loop_->assertInLoopThread();
    updateActiveTime();

    if (channel_->isWriting()) {
        ssize_t n = ::write(channel_->fd(), outputBuffer_.peek(), outputBuffer_.readableBytes());
        if (n > 0) {
            outputBuffer_.retrieve(n);
            if (outputBuffer_.readableBytes() == 0) {
                channel_->disableWriting();
                if (writeCompleteCallback_) {
                    loop_->queueInLoop(std::bind(writeCompleteCallback_, shared_from_this()));
                }
                if (state_ == kDisconnecting) {
                    shutdownInLoop();
                }
            }
        // Issue #6 fix: 区分 EAGAIN 和致命错误，n==0 也当错误处理
        } else {
            if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                handleError();
            }
        }
    }
}

void TcpConnection::handleClose() {
    loop_->assertInLoopThread();
    // Issue #10 fix: 幂等处理，防止 EPOLLERR→handleError→handleClose→closeCallback_→handleClose 重复崩溃
    if (state_ == kDisconnected) return;
    assert(state_ == kConnected || state_ == kDisconnecting);
    setState(kDisconnected);
    channel_->disableAll();

    TcpConnectionPtr guardThis(shared_from_this());
    if (connectionCallback_) {
        connectionCallback_(guardThis);
    }
    if (closeCallback_) {
        closeCallback_(guardThis);
    }
}

void TcpConnection::handleError() {
    int optval;
    socklen_t optlen = sizeof(optval);
    int err = 0;
    if (::getsockopt(channel_->fd(), SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0) {
        err = errno;
    } else {
        err = optval;
    }
    // LOG_ERROR

    // 出现错误后关闭连接
    if (err != 0) {
        handleClose();
    }
}

void TcpConnection::setState(StateE s) {
    state_ = s;
}

void TcpConnection::forceClose() {
    // Issue #8 fix: 仅在 kConnected 状态允许转 kDisconnecting，
    // 防止 forceClose 被多次调用时重复投递 handleClose 导致 assert 崩溃
    StateE expected = kConnected;
    if (state_.compare_exchange_strong(expected, kDisconnecting)) {
        loop_->runInLoop(std::bind(&TcpConnection::handleClose, shared_from_this()));
    }
}

} // namespace rpc