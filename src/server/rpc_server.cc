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
    // Issue #10 fix: 空指针检查
    if (!service) return;
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

void RpcServer::onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    DecodedPacket decoded;

    while (Codec::decode(*buf, decoded)) {
        // FIX: 处理 HEARTBEAT，回复 HEARTBEAT_ACK
        if (decoded.msg_type == MsgType::HEARTBEAT) {
            rpc::Heartbeat ack;
            ack.set_service_name("server");
            ack.set_node_id("server_node");
            ack.set_timestamp(std::time(nullptr));
            std::string packet = Codec::encodeHeartbeat(ack, decoded.req_id);
            conn->send(packet);
            continue;
        }

        if (decoded.msg_type != MsgType::REQUEST) {
            continue;
        }

        auto svcIt = services_.find(decoded.service_name);

        // TraceID 提取（从 metadata 中读取）
        auto traceIt = decoded.rpc_request.metadata().find("trace-id");
        std::string traceId = (traceIt != decoded.rpc_request.metadata().end()) ? traceIt->second : "";
        if (!traceId.empty()) {
            std::cout << "[trace:" << traceId << "] " << decoded.service_name << "." << decoded.method_name << std::endl;
        }

        if (svcIt == services_.end()) {
            RpcResponse resp;
            resp.set_success(false);
            resp.set_error_msg("service not found: " + decoded.service_name);
            std::string response = Codec::encodeResponse(resp, decoded.req_id, Status::FAILED);
            conn->send(response);
            continue;
        }

        RpcResponse resp;
        // Issue #9 fix: 捕获用户 handler 抛出的异常，防止 server crash
        bool ok = false;
        try {
            ok = svcIt->second->callMethod(decoded.method_name, decoded.rpc_request, &resp);
        } catch (const std::exception& e) {
            resp.set_success(false);
            resp.set_error_msg(std::string("handler exception: ") + e.what());
        } catch (...) {
            resp.set_success(false);
            resp.set_error_msg("handler unknown exception");
        }
        (void)ok;

        std::string response = Codec::encodeResponse(resp, decoded.req_id,
            resp.success() ? Status::SUCCESS : Status::FAILED);
        conn->send(response);
    }
}

} // namespace rpc
