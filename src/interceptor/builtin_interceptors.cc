// src/interceptor/builtin_interceptors.cc
#include "interceptor/builtin_interceptors.h"
#include "circuit_breaker/circuit_breaker.h"
#include "rate_limiter/token_bucket.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <functional>

namespace rpc {

// ============================================================================
// CircuitBreakerInterceptor
// ============================================================================
bool CircuitBreakerInterceptor::preHandle(RpcRequest& req) {
    (void)req;
    if (cb_ && !cb_->allowRequest()) {
        std::cerr << "[CBInterceptor] circuit breaker open, request blocked" << std::endl;
        return false;
    }
    return true;
}

// ============================================================================
// RateLimitInterceptor
// ============================================================================
bool RateLimitInterceptor::preHandle(RpcRequest& req) {
    (void)req;
    if (limiter_ && !limiter_->allow()) {
        std::cerr << "[RLInterceptor] rate limit exceeded, request blocked" << std::endl;
        return false;
    }
    return true;
}

// ============================================================================
// TraceInterceptor
// ============================================================================
bool TraceInterceptor::preHandle(RpcRequest& req) {
    // 生成 trace-id（如果调用方已注入则保留，否则新建）
    auto it = req.metadata().find("trace-id");
    if (it == req.metadata().end()) {
        std::string traceId = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()) + "-"
            + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
        (*req.mutable_metadata())["trace-id"] = traceId;
        std::cout << "[Trace] injected trace-id: " << traceId << std::endl;
    }
    return true;
}

void TraceInterceptor::postHandle(const RpcResponse& resp) {
    (void)resp;
    // 可扩展：上报追踪数据到外部收集器
}

// ============================================================================
// LogInterceptor
// ============================================================================
bool LogInterceptor::preHandle(RpcRequest& req) {
    std::cout << "[Log] → request" << (clientId_.empty() ? "" : " [" + clientId_ + "]") << std::endl;
    (void)req;
    return true;
}

void LogInterceptor::postHandle(const RpcResponse& resp) {
    std::cout << "[Log] ← response success=" << resp.success()
              << " err=" << resp.error_msg() << std::endl;
}

} // namespace rpc
