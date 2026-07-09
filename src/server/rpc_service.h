// src/server/rpc_service.h
#ifndef RPC_SERVICE_H
#define RPC_SERVICE_H

#include <string>
#include <unordered_map>
#include <functional>
#include "protocol/rpc_service.pb.h"

namespace rpc {

class RpcService {
public:
    using MethodHandler = std::function<void(const RpcRequest&, RpcResponse*)>;

    virtual ~RpcService() = default;

    // 服务名，子类必须实现
    virtual std::string serviceName() const = 0;

    // 注册方法
    void registerMethod(const std::string& methodName, MethodHandler handler);

    // 调用方法
    bool callMethod(const std::string& methodName,
                    const RpcRequest& request,
                    RpcResponse* response);

protected:
    std::unordered_map<std::string, MethodHandler> methods_;
};

} // namespace rpc

#endif