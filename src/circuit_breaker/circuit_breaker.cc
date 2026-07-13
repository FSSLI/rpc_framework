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
        // double-check: 可能在等锁期间状态变了
        if (state_.load(std::memory_order_relaxed) != OPEN) {
            return state_.load(std::memory_order_relaxed) != OPEN;
        }
        auto elapsed = std::chrono::steady_clock::now() - openedAt_;
        if (elapsed >= openTimeout_) {
            transitionToHalfOpen();
            std::cout << "CircuitBreaker: OPEN → HALF_OPEN (timeout elapsed)" << std::endl;
            return true;  // 允许探测请求
        }
        return false;
    }

    // HALF_OPEN: 允许通过了 allowRequest 的那一个探测请求通过
    return true;
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
    state_.store(OPEN, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        openedAt_ = std::chrono::steady_clock::now();
    }
}

void CircuitBreaker::transitionToHalfOpen() {
    state_.store(HALF_OPEN, std::memory_order_relaxed);
}

void CircuitBreaker::transitionToClosed() {
    state_.store(CLOSED, std::memory_order_relaxed);
    failureCount_.store(0, std::memory_order_relaxed);
}

} // namespace rpc
