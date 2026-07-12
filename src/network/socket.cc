// src/network/socket.cc
#include "socket.h"
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

// sockaddr 是 POSIX 标准定义的通用套接字地址结构体
// 用来统一表示各种网络地址（IPv4、IPv6、Unix域等）
// struct sockaddr {
//     sa_family_t sa_family;    // 地址族（AF_INET、AF_INET6、AF_UNIX 等）
//     char        sa_data[14];  // 地址数据（具体内容取决于地址族）
// };
// 不同地址族对应不同结构体：
//   AF_INET  → sockaddr_in  (IPv4)
//   AF_INET6 → sockaddr_in6 (IPv6)
//   AF_UNIX  → sockaddr_un  (Unix域套接字)

// "Socket 是对 Linux socket API 的面向对象封装。
// 每个方法直接映射到系统调用，主要价值在于 RAII 资源管理和类型安全。

namespace rpc {

// 构造函数：接收一个已创建的文件描述符
// explicit 禁止隐式类型转换，避免 int 被意外转成 
Socket::Socket(int sockfd)
    : sockfd_(sockfd) {
}
//析构函数：RAII自动关闭fd,防止资源泄露
Socket::~Socket() {
    ::close(sockfd_);   //调用系统函数close()关闭连接描述符
}

//绑定本地地址和端口
// 将 sockaddr_in（IPv4专用）通过 reinterpret_cast 转为通用的 sockaddr*
// 这是 POSIX socket API 的设计：用 sockaddr 统一接口，实际传入具体地址结构体
void Socket::bindAddress(const ::sockaddr_in& localaddr) {
    int ret = ::bind(sockfd_, reinterpret_cast<const struct sockaddr*>(&localaddr), sizeof(localaddr));
    if (ret < 0) {
        // LOG_SYSFATAL << "bind";
    }
}

//开始监听端口，等待客户端连接
// SOMAXCONN 是系统默认的最大连接队列长度（通常为 128）
void Socket::listen() {
    int ret = ::listen(sockfd_, SOMAXCONN);  //调用系统函数listen()
    if (ret < 0) {
        // LOG_SYSFATAL << "listen";
    }
}

// 接受客户端连接
// accept4 是 Linux 特有扩展（glibc 2.10+），比 accept + fcntl 少一次系统调用
// 参数 SOCK_NONBLOCK | SOCK_CLOEXEC 同时设置非阻塞和 close-on-exec
// 返回值：新的连接描述符（connfd），peeraddr 输出客户端地址
int Socket::accept(::sockaddr_in* peeraddr) {
    socklen_t addrlen = sizeof(*peeraddr);
    int connfd = ::accept4(sockfd_, reinterpret_cast<struct sockaddr*>(peeraddr),
                           &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
                        //    accept4 的第二个参数是输出参数，内核把客户端 IP 和端口填进去。

    if (connfd < 0) {
        int savedErrno = errno;
        // LOG_SYSERR << "accept";
        // 错误分类：
        // 可恢复错误：通常是临时性的，可以继续 accept
        switch (savedErrno) {
            case EAGAIN:  // 非阻塞模式下暂无连接
            case ECONNABORTED:  //连接被客户端异常终止
            case EINTR:  //被信号中断
            case EPROTO:  //协议错误
            case EPERM:  //防火墙拒绝
            case EMFILE:  //进程fd到达上限
                errno = savedErrno;
                break;
            // 致命错误：程序逻辑或环境问题，不应发生
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