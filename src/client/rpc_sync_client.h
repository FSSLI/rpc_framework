// src/client/rpc_sync_client.h
#ifndef RPC_SYNC_CLIENT_H
#define RPC_SYNC_CLIENT_H

#include <string>
#include <chrono>
#include "client/rpc_async_client.h"

namespace rpc {

class RpcSyncClient {
public:
    RpcSyncClient(const std::string& host, uint16_t port);
    ~RpcSyncClient();

    bool connect();
    void disconnect();

    // 同步调用，底层走异步 + future.wait_for/get
    RpcResponse call(const std::string& service_name,
                     const std::string& method_name,
                     const RpcRequest& request,
                     int timeout_ms = 5000);

    bool connected() const;

private:
    RpcAsyncClient asyncClient_;  // 底层异步客户端，统一网络层
};

} // namespace rpc

#endif