// src/server/rpc_server.h
#ifndef RPC_SERVER_H
#define RPC_SERVER_H

#include <memory>
#include <unordered_map>
#include "network/tcp_server.h"
#include "network/event_loop.h"
#include "codec/rpc_codec.h"

namespace rpc {

class RpcService;

class RpcServer {
public:
    RpcServer(EventLoop* loop, const struct sockaddr_in& listenAddr);
    ~RpcServer();

    // 注册服务：把 UserService/OrderService 等注册到 server
    // 用 shared_ptr 管理生命周期，支持多个服务
    void registerService(std::shared_ptr<RpcService> service);

    // 启动：开启 TcpServer 监听，等待客户端连接
    void start();

    // 新增：设置连接 idle 超时（秒），透传给 TcpServer
    void setIdleTimeout(int seconds) { server_.setIdleTimeout(seconds); }

private:
    // 连接回调：新连接建立/断开时触发
    void onConnection(const TcpConnectionPtr& conn);
    // 消息回调：收到数据时触发，核心处理逻辑
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t);

    EventLoop* loop_; // 主 EventLoop
    TcpServer server_;  // 底层 TCP 服务器，处理网络连接

    // 服务表：service_name → RpcService
    // 如 "UserService" → UserService 实例
    std::unordered_map<std::string, std::shared_ptr<RpcService>> services_;
};

} // namespace rpc

#endif