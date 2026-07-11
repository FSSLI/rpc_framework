// src/server/rpc_server.cc
#include "rpc_server.h"
#include "rpc_service.h"
#include <iostream>
#include "network/tcp_connection.h"



namespace rpc {

RpcServer::RpcServer(EventLoop* loop, const struct sockaddr_in& listenAddr)
    : loop_(loop),
      server_(loop, listenAddr) {
    
    // 绑定 TcpServer 的回调到 RpcServer 的方法
    server_.setConnectionCallback(
        std::bind(&RpcServer::onConnection, this, std::placeholders::_1));
    server_.setMessageCallback(
        std::bind(&RpcServer::onMessage, this, 
            std::placeholders::_1,  // conn
            std::placeholders::_2,  // buf
            std::placeholders::_3));  // receiveTime
}

RpcServer::~RpcServer() = default;

// 注册服务：如 registerService(std::make_shared<UserService>())
void RpcServer::registerService(std::shared_ptr<RpcService> service) {
    services_[service->serviceName()] = service;
}

void RpcServer::start() {
    server_.start();  // 启动 TcpServer，开始监听
}

// 连接回调：打印日志，可扩展连接数限制、统计
void RpcServer::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {  // ← 改为判断连接状态
        std::cout << "RpcServer new connection: " << conn->name() << std::endl;
    } else {
        std::cout << "RpcServer connection closed: " << conn->name() << std::endl;
    }
}

// 消息回调：核心处理逻辑
void RpcServer::onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    std::string packet;

    // 循环拆包：可能一次收到多个完整包
    while (Codec::decodeWithLength(*buf, packet)) {
        // 1. 解码外层 Length，得到完整 packet
        Buffer rpcBuf;
        rpcBuf.append(packet.data(), packet.size());
        
        DecodedPacket decoded;
        std::string service_name, method_name;

        // 2. 解码内部协议，获取 service_name 和 method_name
        if (!Codec::decode(rpcBuf, decoded, &service_name, &method_name)) {
            continue;  // 解码失败，跳过
        }
        
        // 3. 只处理 REQUEST 类型
        if (decoded.msg_type != MsgType::REQUEST) {
            continue;
        }
        
        // 4. 查找服务
        auto svcIt = services_.find(service_name);
        if (svcIt == services_.end()) {
            // 服务不存在，返回错误
            RpcResponse resp;
            resp.set_success(false);
            resp.set_error_msg("service not found: " + service_name);
            std::string response = Codec::encodeResponse(resp, decoded.req_id, Status::FAILED);
            std::string responseWithLen = Codec::encodeWithLength(response);
            conn->send(responseWithLen);
            continue;
        }
        
        // 5. 调用方法
        RpcResponse resp;
        bool ok = svcIt->second->callMethod(method_name, decoded.rpc_request, &resp);
        
        if (!ok) {
            // callMethod 已经设置了 error_msg
        }
        
        // 6. 编码响应
        std::string response = Codec::encodeResponse(resp, decoded.req_id, 
            resp.success() ? Status::SUCCESS : Status::FAILED);
        std::string responseWithLen = Codec::encodeWithLength(response);

        // 7. 发送响应
        conn->send(responseWithLen);
    }
}

} // namespace rpc