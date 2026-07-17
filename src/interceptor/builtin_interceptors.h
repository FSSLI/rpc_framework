// src/interceptor/builtin_interceptors.h
// 内置拦截器：熔断、限流、TraceID、日志
#ifndef BUILTIN_INTERCEPTORS_H
#define BUILTIN_INTERCEPTORS_H

#include "interceptor/interceptor.h"
#include "protocol/rpc_service.pb.h"
#include <string>

namespace rpc {

class CircuitBreaker;
class TokenBucket;

// ============================================================================
// 熔断拦截器：在请求前检查熔断状态
// ============================================================================
class CircuitBreakerInterceptor : public Interceptor {
public:
    explicit CircuitBreakerInterceptor(CircuitBreaker* cb) : cb_(cb) {}
    bool preHandle(RpcRequest& req) override;
private:
    CircuitBreaker* cb_;
};

// ============================================================================
// 限流拦截器：请求前消费令牌
// ============================================================================
class RateLimitInterceptor : public Interceptor {
public:
    explicit RateLimitInterceptor(TokenBucket* limiter) : limiter_(limiter) {}
    bool preHandle(RpcRequest& req) override;
private:
    TokenBucket* limiter_;
};

// ============================================================================
// TraceID 拦截器：请求前注入 trace-id 到 metadata
// ============================================================================
class TraceInterceptor : public Interceptor {
public:
    bool preHandle(RpcRequest& req) override;
    void postHandle(const RpcResponse& resp) override;
};

// ============================================================================
// 日志拦截器：记录请求/响应的 service.method
// ============================================================================
class LogInterceptor : public Interceptor {
public:
    explicit LogInterceptor(const std::string& clientId = "")
        : clientId_(clientId) {}
    bool preHandle(RpcRequest& req) override;
    void postHandle(const RpcResponse& resp) override;
private:
    std::string clientId_;
};

} // namespace rpc

#endif
