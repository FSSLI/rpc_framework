// tcp_server.h
#ifndef TCP_SERVER_H
#define TCP_SERVER_H

#include <functional>
#include <memory>
#include <vector>
#include <map>
#include <string>
#include <cstdint>
#include <netinet/in.h>
#include "buffer.h"


namespace rpc {

class EventLoop;
class Acceptor;
class TcpConnection;
class EventLoopThreadPool;

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;  // ← 加这行
// TcpConnection 的生命周期复杂：
// TcpServer 持有（connections_ map）
// Channel 回调可能引用
// 定时器回调可能引用
// 业务逻辑可能引用
// shared_ptr 自动管理，避免悬空指针。你踩过的 idle 超时 Segmentation Fault 就是因为 lambda 捕获 shared_ptr 延长生命周期。

class TcpServer {
public:
    using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;  //连接回调
    using MessageCallback = std::function<void(const TcpConnectionPtr&, Buffer*, int64_t)>;  //消息回调
    using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;  //写完成回调

    TcpServer(EventLoop* loop, const struct sockaddr_in& listenAddr);  //mainloop，监听地址
    ~TcpServer();

    void setThreadNum(int numThreads);  //// 设置 IO 线程数
    //注册回调函数
    void setConnectionCallback(const ConnectionCallback& cb) { connectionCallback_ = cb; } 
    void setMessageCallback(const MessageCallback& cb) { messageCallback_ = cb; }
    void setWriteCompleteCallback(const WriteCompleteCallback& cb) { writeCompleteCallback_ = cb; }

    void start();  //启动：开启 Acceptor 监听，启动线程池

    // 新增：设置连接 idle 超时（秒），0 表示不检测
    void setIdleTimeout(int seconds) { idleTimeoutSeconds_ = seconds; }  //全局 idle 超时

private:
    void newConnection(int sockfd, const struct sockaddr_in& peerAddr);  //accept 回调
    void removeConnection(const TcpConnectionPtr& conn);  //跨线程安全移除
    void removeConnectionInLoop(const TcpConnectionPtr& conn);  // ← 加这行  在 EventLoop 线程里安全移除

    EventLoop* loop_;  // 主 EventLoop（baseLoop）
    std::string name_;  //服务名称（非 const，支持 inet_ntop 构造体赋值）
    std::unique_ptr<Acceptor> acceptor_;  //监听新连接
    std::shared_ptr<EventLoopThreadPool> threadPool_;  //IO 线程池

    ConnectionCallback connectionCallback_;  
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;

    bool started_;  //启动状态
    int nextConnId_;  //连接 ID 计数器，给每个连接生成唯一名称
    std::map<std::string, TcpConnectionPtr> connections_;  //连接管理表。 记录当前所有活跃连接

    int idleTimeoutSeconds_ = 0;
    uint64_t idleTimerId_ = 0;  // Issue #1 fix: 记录 idle 定时器 ID 用于析构取消
};

} // namespace rpc

#endif