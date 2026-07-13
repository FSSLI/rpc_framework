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
    // 1. 检查连接
    if (!asyncClient_.connected()) {  
        throw std::runtime_error("not connected");
    }

    // Issue #11 fix: 传递 timeout_ms 给异步调用层
    auto future = asyncClient_.asyncCall(service_name, method_name, request, timeout_ms);
    // future 包含：异步任务的结果，可能还没完成

    // 3. 阻塞等待，带超时
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    // status 有三种：
    //   - ready：任务完成，可以 get
    //   - timeout：超时
    //   - deferred：任务延迟执行（这里不会）

    if (status == std::future_status::timeout) {
        throw std::runtime_error("rpc call timeout");
    }
    
    // 4. 获取结果（不会阻塞，因为已经 ready）
    return future.get();
}

bool RpcSyncClient::connected() const {
    return asyncClient_.connected();
}

} // namespace rpc