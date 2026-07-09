// examples/echo/echo_server.cc
#include "network/tcp_server.h"
#include "network/event_loop.h"
#include "network/buffer.h"
#include "network/tcp_connection.h"
#include "codec/rpc_codec.h"
#include "protocol/rpc_service.pb.h"
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

using namespace rpc;

void onConnection(const TcpConnectionPtr& conn) {
    if (conn->getContext() == nullptr) {
        std::cout << "new connection: " << conn->name() << std::endl;
    } else {
        std::cout << "connection closed: " << conn->name() << std::endl;
    }
}

void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    DecodedPacket packet;
    std::string service_name, method_name;
    
    while (Codec::decode(*buf, packet, &service_name, &method_name)) {
        if (packet.msg_type == MsgType::REQUEST) {
            std::cout << "received request: service=" << service_name 
                      << " method=" << method_name 
                      << " req_id=" << packet.req_id << std::endl;
            
            // 解析 EchoRequest
            EchoRequest echoReq;
            if (echoReq.ParseFromString(packet.rpc_request.payload())) {
                std::cout << "echo message: " << echoReq.message() << std::endl;
                
                // 构造 EchoResponse
                EchoResponse echoResp;
                echoResp.set_message(echoReq.message());
                
                RpcResponse rpcResp;
                rpcResp.set_success(true);
                rpcResp.set_payload(echoResp.SerializeAsString());
                
                // 编码发送
                std::string response = Codec::encodeResponse(rpcResp, packet.req_id, Status::SUCCESS);
                conn->send(response);
            }
        }
    }
}

int main() {
    EventLoop loop;
    
    struct sockaddr_in listenAddr;
    memset(&listenAddr, 0, sizeof(listenAddr));
    listenAddr.sin_family = AF_INET;
    listenAddr.sin_port = htons(8888);
    listenAddr.sin_addr.s_addr = INADDR_ANY;
    
    TcpServer server(&loop, listenAddr);
    server.setConnectionCallback(onConnection);
    server.setMessageCallback(onMessage);
    server.start();
    
    std::cout << "RPC echo server listening on 8888" << std::endl;
    loop.loop();
    
    return 0;
}