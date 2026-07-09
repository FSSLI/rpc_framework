// src/client/rpc_sync_client.cc
#include "rpc_sync_client.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <cstring>
#include <iostream>

namespace rpc {

RpcSyncClient::RpcSyncClient(const std::string& host, uint16_t port)
    : host_(host), port_(port), sockfd_(-1), connected_(false), nextReqId_(1) {
}

RpcSyncClient::~RpcSyncClient() {
    disconnect();
}

bool RpcSyncClient::connect() {
    sockfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd_ < 0) {
        std::cerr << "socket failed" << std::endl;
        return false;
    }

    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port_);
    inet_pton(AF_INET, host_.c_str(), &serverAddr.sin_addr);

    if (::connect(sockfd_, reinterpret_cast<struct sockaddr*>(&serverAddr), sizeof(serverAddr)) < 0) {
        std::cerr << "connect failed: " << strerror(errno) << std::endl;
        ::close(sockfd_);
        sockfd_ = -1;
        return false;
    }

    connected_ = true;
    return true;
}

void RpcSyncClient::disconnect() {
    if (sockfd_ >= 0) {
        ::close(sockfd_);
        sockfd_ = -1;
    }
    connected_ = false;
}

RpcResponse RpcSyncClient::call(const std::string& service_name,
                                const std::string& method_name,
                                const RpcRequest& request,
                                int timeout_ms) {
    if (!connected_) {
        throw std::runtime_error("not connected");
    }

    uint64_t reqId = nextReqId_.fetch_add(1);

    // 编码请求
    std::string packet = Codec::encodeRequest(request, reqId, service_name, method_name);
    std::string packetWithLen = Codec::encodeWithLength(packet);

    // 发送
    if (!sendPacket(packetWithLen)) {
        throw std::runtime_error("send failed");
    }

    // 接收响应
    std::string responsePacket;
    if (!recvPacket(responsePacket, timeout_ms)) {
        throw std::runtime_error("recv timeout");
    }

    // 解码 RPC 响应
    Buffer rpcBuf;
    rpcBuf.append(responsePacket.data(), responsePacket.size());
    
    DecodedPacket decoded;
    if (!Codec::decode(rpcBuf, decoded)) {
        throw std::runtime_error("decode failed");
    }

    if (decoded.msg_type != MsgType::RESPONSE) {
        throw std::runtime_error("unexpected response type");
    }

    return decoded.rpc_response;
}

bool RpcSyncClient::sendPacket(const std::string& packet) {
    size_t sent = 0;
    while (sent < packet.size()) {
        ssize_t n = ::send(sockfd_, packet.data() + sent, packet.size() - sent, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += n;
    }
    return true;
}

bool RpcSyncClient::recvPacket(std::string& packet, int timeout_ms) {
    Buffer buf;
    char tmp[4096];
    
    while (true) {
        if (!waitForResponse(timeout_ms)) {
            return false;
        }
        
        ssize_t n = ::recv(sockfd_, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            return false;
        }
        
        buf.append(tmp, n);
        
        if (Codec::decodeWithLength(buf, packet)) {
            return true;
        }
    }
}

bool RpcSyncClient::waitForResponse(int timeout_ms) {
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(sockfd_, &readfds);

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    int ret = ::select(sockfd_ + 1, &readfds, nullptr, nullptr, &tv);
    return ret > 0;
}

bool RpcSyncClient::connected() const {
    return connected_;
}

} // namespace rpc