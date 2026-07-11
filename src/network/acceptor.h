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
    using NewConnectionCallback = std::function<void(int sockfd, const struct sockaddr_in&)>;  //新连接回调函数
    // 新连接回调：sockfd = 新连接描述符，sockaddr_in = 客户端地址

    Acceptor(EventLoop* loop, const struct sockaddr_in& listenAddr);   // 构造函数：绑定主 EventLoop 和监听地址
    ~Acceptor();

    void setNewConnectionCallback(const NewConnectionCallback& cb) {  //设置回调函数
        newConnectionCallback_ = cb;
    }

    void listen();  //监听
    bool listening() const { return listening_; }  //当前状态

private:
    void handleRead();  // Channel 可读回调：accept 新连接，调用 newConnectionCallback_

    EventLoop* loop_;  //主循环
    std::unique_ptr<Socket> listenSocket_;  //监听套接字
    std::unique_ptr<Channel> listenChannel_;  //绑定的channel
    NewConnectionCallback newConnectionCallback_;  // 回调函数
    bool listening_;  //当前状态
};

} // namespace rpc

#endif