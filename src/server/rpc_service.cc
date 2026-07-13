// src/server/rpc_service.cc
#include "rpc_service.h"
#include <iostream>

namespace rpc {

// 注册方法：覆盖同名方法
void RpcService::registerMethod(const std::string& methodName, MethodHandler handler) {
    // Issue #10 fix: 空检查
    if (methodName.empty() || !handler) return;
    methods_[methodName] = std::move(handler);
}

// 调用方法：
// 1. 在 methods_ 里查找 methodName
// 2. 找到：执行 handler，返回 true
// 3. 找不到：设置错误信息，返回 false
bool RpcService::callMethod(const std::string& methodName,
                            const RpcRequest& request,
                            RpcResponse* response) {
    auto it = methods_.find(methodName);
    if (it == methods_.end()) {
        response->set_success(false);
        response->set_error_msg("method not found: " + methodName);
        return false;
    }
    
    it->second(request, response);   // 执行方法
    return true;
}

} // namespace rpc