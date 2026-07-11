// src/server/rpc_server.cc
#include "rpc_server.h"
#include "rpc_service.h"
#include <iostream>
#include "network/tcp_connection.h"

namespace rpc {

RpcServer::RpcServer(EventLoop* loop, const struct sockaddr_in& listenAddr)
    : loop_(loop),
      server_(loop, listenAddr) {

    server_.setConnectionCallback(
        std::bind(&RpcServer::onConnection, this, std::placeholders::_1));
    server_.setMessageCallback(
        std::bind(&RpcServer::onMessage, this, 
            std::placeholders::_1,
            std::placeholders::_2,
            std::placeholders::_3));
}

RpcServer::~RpcServer() = default;

void RpcServer::registerService(std::shared_ptr<RpcService> service) {
    services_[service->serviceName()] = service;
}

void RpcServer::start() {
    server_.start();
}

void RpcServer::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        std::cout << "RpcServer new connection: " << conn->name() << std::endl;
    } else {
        std::cout << "RpcServer connection closed: " << conn->name() << std::endl;
    }
}

// ============================================================================
// onMessage - 消息回调：核心处理逻辑
// ============================================================================
// 新协议：Codec::decode 直接从 Buffer 中解码完整包
// 无需先 decodeWithLength 再 decode
// ============================================================================
void RpcServer::onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    DecodedPacket decoded;

    // 循环解码：可能一次收到多个完整包
    while (Codec::decode(*buf, decoded)) {
        // 1. 只处理 REQUEST 类型
        if (decoded.msg_type != MsgType::REQUEST) {
            // TODO: 处理 HEARTBEAT 类型（回复 HEARTBEAT_ACK）
            continue;
        }

        // 2. 查找服务
        auto svcIt = services_.find(decoded.service_name);
        if (svcIt == services_.end()) {
            RpcResponse resp;
            resp.set_success(false);
            resp.set_error_msg("service not found: " + decoded.service_name);
            std::string response = Codec::encodeResponse(resp, decoded.req_id, Status::FAILED);
            conn->send(response);
            continue;
        }

        // 3. 调用方法
        RpcResponse resp;
        bool ok = svcIt->second->callMethod(decoded.method_name, decoded.rpc_request, &resp);

        if (!ok) {
            // callMethod 已经设置了 error_msg
        }

        // 4. 编码并发送响应（直接发送完整包，无需加长度头）
        std::string response = Codec::encodeResponse(resp, decoded.req_id, 
            resp.success() ? Status::SUCCESS : Status::FAILED);
        conn->send(response);
    }
}

} // namespace rpc