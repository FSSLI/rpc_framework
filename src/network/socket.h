// src/network/socket.h
#ifndef SOCKET_H
#define SOCKET_H

#include <netinet/in.h>

namespace rpc {

class Socket {
public:
    explicit Socket(int sockfd);
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    int fd() const { return sockfd_; }

    void bindAddress(const struct sockaddr_in& localaddr);
    void listen();
    int accept(struct sockaddr_in* peeraddr);

    void shutdownWrite();

    void setTcpNoDelay(bool on);
    void setReuseAddr(bool on);
    void setReusePort(bool on);
    void setKeepAlive(bool on);

private:
    const int sockfd_;
};

} // namespace rpc

#endif