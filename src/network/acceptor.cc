// src/network/acceptor.cc
#include "acceptor.h"
#include "event_loop.h"
#include "channel.h"
#include "socket.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>  // ← 加这行

namespace rpc {

// SOCK_NONBLOCK：创建时就非阻塞
// SOCK_CLOEXEC：close-on-exec
// IPPROTO_TCP：TCP 协议

Acceptor::Acceptor(EventLoop* loop, const struct sockaddr_in& listenAddr)  
    : loop_(loop),
      listenSocket_(new Socket(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP))), 
      listenChannel_(new Channel(loop, listenSocket_->fd())),
      listening_(false) {
    
    listenSocket_->setReuseAddr(true);
    listenSocket_->setReusePort(true);
    listenSocket_->bindAddress(listenAddr);
    
    listenChannel_->setReadCallback(std::bind(&Acceptor::handleRead, this));  
    // Channel 的可读回调绑定到 Acceptor::handleRead，不是 TcpConnection::handleRead
}

Acceptor::~Acceptor() {
    listenChannel_->disableAll();  //注销所有事件
    listenChannel_->remove();  //从epoll移除
}

void Acceptor::listen() {   //启动监听
    loop_->assertInLoopThread();  //在主循环监听
    listening_ = true;  
    listenSocket_->listen();  //调用系统调用listen
    listenChannel_->enableReading();  //注册读事件
}

void Acceptor::handleRead() {
    loop_->assertInLoopThread();
    struct sockaddr_in peerAddr;  //accept4 的第二个参数是输出参数，内核把客户端 IP 和端口填进去。
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