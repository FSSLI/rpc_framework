// src/circuit_breaker/circuit_breaker.cc
#include "circuit_breaker/circuit_breaker.h"
#include <iostream>

namespace rpc {

CircuitBreaker::CircuitBreaker(int failureThreshold, int openTimeoutSec)
    : failureThreshold_(failureThreshold),
      openTimeout_(std::chrono::seconds(openTimeoutSec)) {
}

bool CircuitBreaker::allowRequest() {
    State s = state_.load(std::memory_order_relaxed);

    if (s == CLOSED) {
        return true;
    }

    if (s == OPEN) {
        std::lock_guard<std::mutex> lock(mutex_);
        // 等锁期间状态可能已被其他线程改变
        s = state_.load(std::memory_order_relaxed);
        if (s == CLOSED) return true;
        if (s == HALF_OPEN) {
            int expected = 0;
            return halfOpenRequests_.compare_exchange_strong(expected, 1);
        }
        // 仍是 OPEN，检查超时
        auto elapsed = std::chrono::steady_clock::now() - openedAt_;
        if (elapsed >= openTimeout_) {
            transitionToHalfOpen();
            std::cout << "CircuitBreaker: OPEN → HALF_OPEN (timeout elapsed)" << std::endl;
            return true;  // 允许探测请求
        }
        return false;
    }

    // HALF_OPEN: 只允许一个探测请求通过。若探测超时未返回，重置计数
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto elapsed = std::chrono::steady_clock::now() - openedAt_;
        if (elapsed >= halfOpenTimeout_) {
            halfOpenRequests_.store(0, std::memory_order_relaxed);
            openedAt_ = std::chrono::steady_clock::now();  // 重置超时窗口
        }
    }
    int expected = 0;
    if (halfOpenRequests_.compare_exchange_strong(expected, 1)) {
        return true;
    }
    return false;
}

void CircuitBreaker::recordSuccess() {
    State s = state_.load(std::memory_order_relaxed);

    if (s == CLOSED) {
        failureCount_.store(0, std::memory_order_relaxed);
    } else if (s == HALF_OPEN) {
        transitionToClosed();
        std::cout << "CircuitBreaker: HALF_OPEN → CLOSED (probe succeeded)" << std::endl;
    }
    // OPEN 状态下不应该有请求成功，忽略
}

void CircuitBreaker::recordFailure() {
    State s = state_.load(std::memory_order_relaxed);

    if (s == CLOSED) {
        int count = failureCount_.fetch_add(1, std::memory_order_relaxed) + 1;
        if (count >= failureThreshold_) {
            transitionToOpen();
            std::cout << "CircuitBreaker: CLOSED → OPEN (" << count
                      << " consecutive failures)" << std::endl;
        }
    } else if (s == HALF_OPEN) {
        // 探测失败，重新熔断
        transitionToOpen();
        std::cout << "CircuitBreaker: HALF_OPEN → OPEN (probe failed)" << std::endl;
    }
    // OPEN 状态下不应有请求，忽略
}

void CircuitBreaker::transitionToOpen() {
    // Issue #1 fix: 先记时间再改状态，防止 allowRequest 读到 OPEN+epoch
    {
        std::lock_guard<std::mutex> lock(mutex_);
        openedAt_ = std::chrono::steady_clock::now();
    }
    state_.store(OPEN, std::memory_order_relaxed);
}

void CircuitBreaker::transitionToHalfOpen() {
    halfOpenRequests_.store(0, std::memory_order_relaxed);
    state_.store(HALF_OPEN, std::memory_order_relaxed);
}

void CircuitBreaker::transitionToClosed() {
    state_.store(CLOSED, std::memory_order_relaxed);
    failureCount_.store(0, std::memory_order_relaxed);
    halfOpenRequests_.store(0, std::memory_order_relaxed);
}

} // namespace rpc
