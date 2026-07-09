// src/server/rpc_service.cc
#include "rpc_service.h"
#include <iostream>

namespace rpc {

void RpcService::registerMethod(const std::string& methodName, MethodHandler handler) {
    methods_[methodName] = std::move(handler);
}

bool RpcService::callMethod(const std::string& methodName,
                            const RpcRequest& request,
                            RpcResponse* response) {
    auto it = methods_.find(methodName);
    if (it == methods_.end()) {
        response->set_success(false);
        response->set_error_msg("method not found: " + methodName);
        return false;
    }
    
    it->second(request, response);
    return true;
}

} // namespace rpc