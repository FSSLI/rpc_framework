// src/client/rpc_sync_client.h
#ifndef RPC_SYNC_CLIENT_H
#define RPC_SYNC_CLIENT_H

#include <string>
#include <mutex>
#include <atomic>
#include <chrono>

#include "codec/rpc_codec.h"
#include "protocol/rpc_service.pb.h"

namespace rpc {

class RpcSyncClient {
public:
    RpcSyncClient(const std::string& host, uint16_t port);
    ~RpcSyncClient();

    bool connect();
    void disconnect();

    RpcResponse call(const std::string& service_name,
                     const std::string& method_name,
                     const RpcRequest& request,
                     int timeout_ms = 5000);

    bool connected() const;

private:
    bool sendPacket(const std::string& packet);
    bool recvPacket(std::string& packet, int timeout_ms);
    bool waitForResponse(int timeout_ms);

    std::string host_;
    uint16_t port_;
    int sockfd_;
    bool connected_;
    std::atomic<uint64_t> nextReqId_;

    // 接收缓冲区
    std::string recvBuf_;
    std::mutex recvMutex_;
};

} // namespace rpc

#endif