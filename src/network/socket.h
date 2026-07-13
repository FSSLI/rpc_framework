// src/network/socket.h
#ifndef SOCKET_H
#define SOCKET_H

#include <netinet/in.h>

// struct sockaddr_in {
//     sa_family_t    sin_family;   // 地址族，AF_INET
//     in_port_t      sin_port;     // 端口号（网络字节序）
//     struct in_addr sin_addr;     // IP地址
//     char           sin_zero[8];   // 填充
// };

namespace rpc {

class Socket {
public:
    explicit Socket(int sockfd);  //explicit 防止隐式类型转换
    ~Socket();

    Socket(const Socket&) = delete;  //禁止拷贝构造
    Socket& operator=(const Socket&) = delete;  //禁止赋值运算符

    int fd() const { return sockfd_; }  //获取连接描述符

    void bindAddress(const ::sockaddr_in& localaddr);   // 绑定本地地址（含IP和端口）
    void listen(); // 开始监听，等待客户端连接
    int accept(::sockaddr_in* peeraddr);  // 接受连接，返回客户端fd，peeraddr输出客户端地址

    void shutdownWrite();  //关闭写通道

    void setTcpNoDelay(bool on);  // 禁用Nagle算法，降低延迟（true=禁用）
    void setReuseAddr(bool on);  // 设置SO_REUSEADDR，允许地址快速重用
    void setReusePort(bool on);  // 设置SO_REUSEPORT，允许多进程绑定同一端口
    void setKeepAlive(bool on);  // 启用TCP保活探测，检测死连接

    // 新增：静态工具方法
    static int getSocketError(int sockfd);
    static struct sockaddr_in getPeerAddr(int sockfd);
    static struct sockaddr_in getLocalAddr(int sockfd);

private:
    const int sockfd_;  // 文件描述符，构造后不可变
};

} // namespace rpc

#endif