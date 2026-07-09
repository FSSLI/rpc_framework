// src/client/rpc_sync_client.cc
#include "client/rpc_sync_client.h"
#include <iostream>

namespace rpc {

RpcSyncClient::RpcSyncClient(const std::string& host, uint16_t port)
    : asyncClient_(host, port) {
}

RpcSyncClient::~RpcSyncClient() {
    disconnect();
}

bool RpcSyncClient::connect() {
    return asyncClient_.connect();
}

void RpcSyncClient::disconnect() {
    asyncClient_.disconnect();
}

RpcResponse RpcSyncClient::call(const std::string& service_name,
                                const std::string& method_name,
                                const RpcRequest& request,
                                int timeout_ms) {
    if (!asyncClient_.connected()) {
        throw std::runtime_error("not connected");
    }

    // 底层走异步，获取 future
    auto future = asyncClient_.asyncCall(service_name, method_name, request);

    // 超时等待
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    if (status == std::future_status::timeout) {
        throw std::runtime_error("rpc call timeout");
    }
    
    // status == ready，安全获取结果
    return future.get();
}

bool RpcSyncClient::connected() const {
    return asyncClient_.connected();
}

} // namespace rpc