// src/circuit_breaker/circuit_breaker.h
#ifndef CIRCUIT_BREAKER_H
#define CIRCUIT_BREAKER_H

#include <atomic>
#include <mutex>
#include <chrono>

namespace rpc {

class CircuitBreaker {
public:
    enum State { CLOSED, OPEN, HALF_OPEN };

    // failureThreshold: 连续失败 N 次后熔断
    // openTimeoutSec: 熔断后多少秒进入半开状态
    explicit CircuitBreaker(int failureThreshold = 5, int openTimeoutSec = 10);

    // 返回 true 表示允许发起请求
    bool allowRequest();

    // 记录单次请求结果
    void recordSuccess();
    void recordFailure();

    State state() const { return state_.load(std::memory_order_relaxed); }
    int failureCount() const { return failureCount_.load(std::memory_order_relaxed); }

private:
    void transitionToOpen();
    void transitionToHalfOpen();
    void transitionToClosed();

    std::atomic<State> state_{CLOSED};
    std::atomic<int> failureCount_{0};
    int failureThreshold_;
    std::chrono::milliseconds openTimeout_;

    // 保护 OPEN → HALF_OPEN 的转换
    std::mutex mutex_;
    std::chrono::steady_clock::time_point openedAt_;
    std::atomic<int> halfOpenRequests_{0};  // 半开状态只允许 1 个探测
    std::chrono::milliseconds halfOpenTimeout_{5000};  // 探测超时 5s 后重试
};

} // namespace rpc

#endif
