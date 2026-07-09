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
      context_(nullptr) {
    
    channel_->setReadCallback(std::bind(&TcpConnection::handleRead, this));
    channel_->setWriteCallback(std::bind(&TcpConnection::handleWrite, this));
    channel_->setCloseCallback(std::bind(&TcpConnection::handleClose, this));
    channel_->setErrorCallback(std::bind(&TcpConnection::handleError, this));
    
    socket_->setKeepAlive(true);
}

TcpConnection::~TcpConnection() {
    // LOG_DEBUG
}

void TcpConnection::connectEstablished() {
    loop_->assertInLoopThread();
    assert(state_ == kConnecting);
    setState(kConnected);
    channel_->enableReading();
    std::cout << "connectEstablished: " << name_ << " fd=" << channel_->fd() << std::endl;  // ← 加
    
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

void TcpConnection::send(const std::string& message) {
    if (state_ == kConnected) {
        if (loop_->isInLoopThread()) {
            sendInLoop(message);
        } else {
            loop_->runInLoop(std::bind(&TcpConnection::sendInLoop, this, message));
        }
    }
}

void TcpConnection::send(Buffer* buf) {
    if (state_ == kConnected) {
        if (loop_->isInLoopThread()) {
            sendInLoop(buf->retrieveAllAsString());
        } else {
            loop_->runInLoop(std::bind(&TcpConnection::sendInLoop, this, buf->retrieveAllAsString()));
        }
    }
}

void TcpConnection::shutdown() {
    if (state_ == kConnected) {
        setState(kDisconnecting);
        loop_->runInLoop(std::bind(&TcpConnection::shutdownInLoop, this));
    }
}

void TcpConnection::sendInLoop(const std::string& message) {
    loop_->assertInLoopThread();
    ssize_t nwrote = 0;
    size_t remaining = message.size();
    bool faultError = false;
    
    if (state_ == kDisconnected) {
        // LOG_WARN << "disconnected, give up writing";
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
            if (errno != EWOULDBLOCK) {
                // LOG_SYSERR << "TcpConnection::sendInLoop";
                if (errno == EPIPE || errno == ECONNRESET) {
                    faultError = true;
                }
            }
        }
    }
    
    if (!faultError && remaining > 0) {
        outputBuffer_.append(message.data() + nwrote, remaining);
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
    std::cout << "handleRead: " << name_ << std::endl;  // ← 加
    int savedErrno = 0;
    int64_t receiveTime = 0; // 或用 time(nullptr)
    ssize_t n = inputBuffer_.readFd(channel_->fd(), &savedErrno);
    std::cout << "readFd n=" << n << " errno=" << savedErrno << std::endl;  // ← 加
    if (n > 0) {
        if (messageCallback_) {
            messageCallback_(shared_from_this(), &inputBuffer_, receiveTime);
        }
    } else if (n == 0) {
        handleClose();
    } else {
        errno = savedErrno;
        // LOG_SYSERR << "TcpConnection::handleRead";
        handleError();
    }
}

void TcpConnection::handleWrite() {
    loop_->assertInLoopThread();
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
        } else {
            // LOG_SYSERR << "TcpConnection::handleWrite";
        }
    }
}

void TcpConnection::handleClose() {
    loop_->assertInLoopThread();
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
    // LOG_ERROR << "TcpConnection::handleError [" << name_ << "] - SO_ERROR = " << err;
}

void TcpConnection::setState(StateE s) {
    state_ = s;
}

} // namespace rpc