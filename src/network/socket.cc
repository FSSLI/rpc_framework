// src/network/socket.cc
#include "socket.h"
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

namespace rpc {

Socket::Socket(int sockfd)
    : sockfd_(sockfd) {
}

Socket::~Socket() {
    ::close(sockfd_);
}

void Socket::bindAddress(const struct sockaddr_in& localaddr) {
    int ret = ::bind(sockfd_, reinterpret_cast<const struct sockaddr*>(&localaddr), sizeof(localaddr));
    if (ret < 0) {
        // LOG_SYSFATAL << "bind";
    }
}

void Socket::listen() {
    int ret = ::listen(sockfd_, SOMAXCONN);
    if (ret < 0) {
        // LOG_SYSFATAL << "listen";
    }
}

int Socket::accept(struct sockaddr_in* peeraddr) {
    socklen_t addrlen = sizeof(*peeraddr);
    int connfd = ::accept4(sockfd_, reinterpret_cast<struct sockaddr*>(peeraddr),
                           &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (connfd < 0) {
        int savedErrno = errno;
        // LOG_SYSERR << "accept";
        switch (savedErrno) {
            case EAGAIN:
            case ECONNABORTED:
            case EINTR:
            case EPROTO:
            case EPERM:
            case EMFILE:
                errno = savedErrno;
                break;
            case EBADF:
            case EFAULT:
            case EINVAL:
            case ENFILE:
            case ENOBUFS:
            case ENOMEM:
            case ENOTSOCK:
            case EOPNOTSUPP:
                // LOG_FATAL << "unexpected accept error";
                break;
            default:
                // LOG_FATAL << "unknown accept error";
                break;
        }
    }
    return connfd;
}

void Socket::shutdownWrite() {
    ::shutdown(sockfd_, SHUT_WR);
}

void Socket::setTcpNoDelay(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
}

void Socket::setReuseAddr(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
}

void Socket::setReusePort(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));
}

void Socket::setKeepAlive(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval));
}

} // namespace rpc