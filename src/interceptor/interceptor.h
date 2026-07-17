// src/interceptor/interceptor.h
// 责任链模式：请求依次经过拦截器链的 preHandle，响应逆序经过 postHandle
#ifndef INTERCEPTOR_H
#define INTERCEPTOR_H

#include <memory>
#include <vector>
#include <functional>

namespace rpc {

class RpcRequest;
class RpcResponse;

class Interceptor {
public:
    virtual ~Interceptor() = default;

    // 请求前拦截，返回 false 表示拒绝请求（直接返回错误响应）
    virtual bool preHandle(RpcRequest& req) { return true; }

    // 响应后拦截（逆序执行）
    virtual void postHandle(const RpcResponse& resp) {}
};

class InterceptorChain {
public:
    using Handler = std::function<RpcResponse(RpcRequest&)>;

    void addInterceptor(std::shared_ptr<Interceptor> interceptor) {
        interceptors_.push_back(std::move(interceptor));
    }

    size_t size() const { return interceptors_.size(); }

    // 执行拦截器链：
    //   preHandle 顺序执行 → 任一返回 false 则立即返回错误响应
    //   → handler → postHandle 逆序执行
    RpcResponse execute(RpcRequest& req, Handler handler);

private:
    std::vector<std::shared_ptr<Interceptor>> interceptors_;
};

} // namespace rpc

#endif
