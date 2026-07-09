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

    // 注册服务
    void registerService(std::shared_ptr<RpcService> service);

    // 启动
    void start();

    // 新增：设置连接 idle 超时（秒），透传给 TcpServer
    void setIdleTimeout(int seconds) { server_.setIdleTimeout(seconds); }

private:
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t);

    EventLoop* loop_;
    TcpServer server_;
    std::unordered_map<std::string, std::shared_ptr<RpcService>> services_;
};

} // namespace rpc

#endif