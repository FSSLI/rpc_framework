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
    // 方法处理器类型：接收 RpcRequest，填充 RpcResponse
    using MethodHandler = std::function<void(const RpcRequest&, RpcResponse*)>;

    virtual ~RpcService() = default;

    // 纯虚函数：子类必须实现，返回服务名（如 "UserService"）
    virtual std::string serviceName() const = 0;

    // 注册方法：把 methodName 和对应的处理函数绑定
    void registerMethod(const std::string& methodName, MethodHandler handler);

    // 调用方法：根据 methodName 查找并执行，找不到返回 false
    bool callMethod(const std::string& methodName,
                    const RpcRequest& request,
                    RpcResponse* response);

protected:
    // 方法表：methodName → MethodHandler
    // 子类在构造函数里注册自己的方法
    std::unordered_map<std::string, MethodHandler> methods_;
};

} // namespace rpc

#endif