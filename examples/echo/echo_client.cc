// examples/echo/echo_client.cc
#include "network/tcp_connection.h"
#include "network/event_loop.h"
#include "network/buffer.h"
#include "codec/rpc_codec.h"
#include "protocol/rpc_service.pb.h"
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <sys/socket.h>

using namespace rpc;

void onMessage(Buffer* buf, int64_t) {
    DecodedPacket packet;
    while (Codec::decode(*buf, packet)) {
        if (packet.msg_type == MsgType::RESPONSE) {
            std::cout << "received response: req_id=" << packet.req_id << std::endl;
            
            EchoResponse echoResp;
            if (echoResp.ParseFromString(packet.rpc_response.payload())) {
                std::cout << "echo: " << echoResp.message() << std::endl;
            }
        }
    }
}

int main() {
    EventLoop loop;
    
    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(8888);
    inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);
    
    // 手动创建 socket 连接（TcpClient 还没实现）
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        std::cerr << "socket failed" << std::endl;
        return 1;
    }
    
    if (::connect(sockfd, reinterpret_cast<struct sockaddr*>(&serverAddr), sizeof(serverAddr)) < 0) {
        std::cerr << "connect failed" << std::endl;
        return 1;
    }
    
    std::cout << "connected to server" << std::endl;
    
    // 构造 EchoRequest
    EchoRequest echoReq;
    echoReq.set_message("hello rpc");
    
    RpcRequest rpcReq;
    rpcReq.set_payload(echoReq.SerializeAsString());
    
    // 编码发送
    std::string request = Codec::encodeRequest(rpcReq, 1, "EchoService", "Echo");
    
    ssize_t n = ::write(sockfd, request.data(), request.size());
    std::cout << "sent " << n << " bytes" << std::endl;
    
    // 接收响应
    Buffer buf;
    char tmp[1024];
    while (true) {
        n = ::read(sockfd, tmp, sizeof(tmp));
        if (n > 0) {
            buf.append(tmp, n);
            onMessage(&buf, 0);
        } else if (n == 0) {
            break;
        } else {
            break;
        }
    }
    
    ::close(sockfd);
    return 0;
}