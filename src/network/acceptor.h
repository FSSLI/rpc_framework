// src/network/acceptor.h
#ifndef ACCEPTOR_H
#define ACCEPTOR_H

#include <functional>
#include <memory>
#include <netinet/in.h>  // ← 加这行

namespace rpc {

class EventLoop;
class Channel;
class Socket;

class Acceptor {
public:
    using NewConnectionCallback = std::function<void(int sockfd, const struct sockaddr_in&)>;

    Acceptor(EventLoop* loop, const struct sockaddr_in& listenAddr);
    ~Acceptor();

    void setNewConnectionCallback(const NewConnectionCallback& cb) {
        newConnectionCallback_ = cb;
    }

    void listen();
    bool listening() const { return listening_; }

private:
    void handleRead();

    EventLoop* loop_;
    std::unique_ptr<Socket> listenSocket_;
    std::unique_ptr<Channel> listenChannel_;
    NewConnectionCallback newConnectionCallback_;
    bool listening_;
};

} // namespace rpc

#endif