// src/network/acceptor.cc
#include "acceptor.h"
#include "event_loop.h"
#include "channel.h"
#include "socket.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>  // ← 加这行
#include <netinet/in.h>

namespace rpc {

Acceptor::Acceptor(EventLoop* loop, const struct sockaddr_in& listenAddr)
    : loop_(loop),
      listenSocket_(new Socket(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP))),
      listenChannel_(new Channel(loop, listenSocket_->fd())),
      listening_(false) {
    
    listenSocket_->setReuseAddr(true);
    listenSocket_->setReusePort(true);
    listenSocket_->bindAddress(listenAddr);
    
    listenChannel_->setReadCallback(std::bind(&Acceptor::handleRead, this));
}

Acceptor::~Acceptor() {
    listenChannel_->disableAll();
    listenChannel_->remove();
}

void Acceptor::listen() {
    loop_->assertInLoopThread();
    listening_ = true;
    listenSocket_->listen();
    listenChannel_->enableReading();
}

void Acceptor::handleRead() {
    loop_->assertInLoopThread();
    struct sockaddr_in peerAddr;
    int connfd = listenSocket_->accept(&peerAddr);
    if (connfd >= 0) {
        if (newConnectionCallback_) {
            newConnectionCallback_(connfd, peerAddr);
        } else {
            ::close(connfd);
        }
    }
}

} // namespace rpc