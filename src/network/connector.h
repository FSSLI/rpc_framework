// src/network/connector.h
#ifndef CONNECTOR_H
#define CONNECTOR_H

#include <functional>
#include <memory>
#include <atomic>
#include <netinet/in.h>

namespace rpc {

class Channel;
class EventLoop;

class Connector : public std::enable_shared_from_this<Connector> {
public:
    using NewConnectionCallback = std::function<void(int sockfd)>;

    // 注意：Connector 必须由 shared_ptr 管理，因为 retry() 定时器通过
    // shared_from_this() → weak_ptr 防止 use-after-free。用裸指针或 unique_ptr 会崩溃。
    Connector(EventLoop* loop, const struct sockaddr_in& serverAddr);
    ~Connector();

    void setNewConnectionCallback(const NewConnectionCallback& cb) {
        newConnectionCallback_ = cb;
    }

    void start();      // 开始连接（线程安全，内部转发到 loop 线程）
    void restart();    // 断开后重连
    void stop();       // 停止连接和重连

    const struct sockaddr_in& serverAddress() const { return serverAddr_; }

private:
    enum class State {
        kDisconnected,   // 未连接 / 已断开
        kConnecting,     // 正在连接
        kConnected       // 已连接（连接成功后不再由 Connector 管理）
    };

    void startInLoop();
    void stopInLoop();
    void connect();              // 发起非阻塞 connect
    void connecting(int sockfd); // 注册到 epoll 监听可写事件
    void handleWrite();          // connect 完成（可写事件触发）
    void handleError();          // connect 出错
    void retry(int sockfd);      // 指数退避重连

    int removeAndResetChannel(); // 从 epoll 移除 channel，关闭 fd
    void resetChannel();         // 创建新 channel（当前未使用，预留）

    void setState(State s);

    EventLoop* loop_;
    struct sockaddr_in serverAddr_;

    std::atomic<State> state_;
    std::atomic<bool> connect_;  // 用户是否要求连接（stop 后设为 false）

    std::unique_ptr<Channel> channel_;
    int retryDelayMs_;

    NewConnectionCallback newConnectionCallback_;

    static const int kMaxRetryDelayMs = 30000;   // 最大 30s
    static const int kInitRetryDelayMs = 500;    // 初始 500ms
};

} // namespace rpc

#endif