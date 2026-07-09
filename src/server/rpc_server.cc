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
        std::bind(&RpcServer::onMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
}

RpcServer::~RpcServer() = default;

void RpcServer::registerService(std::shared_ptr<RpcService> service) {
    services_[service->serviceName()] = service;
}

void RpcServer::start() {
    server_.start();
}

void RpcServer::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {  // ← 改为判断连接状态
        std::cout << "RpcServer new connection: " << conn->name() << std::endl;
    } else {
        std::cout << "RpcServer connection closed: " << conn->name() << std::endl;
    }
}

void RpcServer::onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    std::string packet;
    while (Codec::decodeWithLength(*buf, packet)) {
        Buffer rpcBuf;
        rpcBuf.append(packet.data(), packet.size());
        
        DecodedPacket decoded;
        std::string service_name, method_name;
        if (!Codec::decode(rpcBuf, decoded, &service_name, &method_name)) {
            continue;
        }
        
        if (decoded.msg_type != MsgType::REQUEST) {
            continue;
        }
        
        // 查找服务
        auto svcIt = services_.find(service_name);
        if (svcIt == services_.end()) {
            RpcResponse resp;
            resp.set_success(false);
            resp.set_error_msg("service not found: " + service_name);
            std::string response = Codec::encodeResponse(resp, decoded.req_id, Status::FAILED);
            std::string responseWithLen = Codec::encodeWithLength(response);
            conn->send(responseWithLen);
            continue;
        }
        
        // 调用方法
        RpcResponse resp;
        bool ok = svcIt->second->callMethod(method_name, decoded.rpc_request, &resp);
        
        if (!ok) {
            // callMethod 已经设置了 error_msg
        }
        
        // 发送响应
        std::string response = Codec::encodeResponse(resp, decoded.req_id, 
            resp.success() ? Status::SUCCESS : Status::FAILED);
        std::string responseWithLen = Codec::encodeWithLength(response);
        conn->send(responseWithLen);
    }
}

} // namespace rpc