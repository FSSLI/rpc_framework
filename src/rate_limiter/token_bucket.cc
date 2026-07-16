// src/rate_limiter/token_bucket.cc
#include "rate_limiter/token_bucket.h"

namespace rpc {

TokenBucket::TokenBucket(double rate, double capacity)
    : rate_(rate),
      capacity_(capacity),
      tokens_(capacity),
      lastRefillTime_(std::chrono::steady_clock::now()) {
}

bool TokenBucket::allow() {
    std::lock_guard<std::mutex> lock(mutex_);
    refill();

    if (tokens_ >= 1.0) {
        tokens_ -= 1.0;
        return true;
    }
    return false;
}

void TokenBucket::setRate(double rate, double capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    refill();
    rate_ = rate;
    if (capacity >= 0) capacity_ = capacity;  // Issue #9: 允许设为 0
    if (tokens_ > capacity_) tokens_ = capacity_;
}

double TokenBucket::availableTokens() const {
    std::lock_guard<std::mutex> lock(mutex_);
    refill();  // refill 是 const 方法
    return tokens_;
}

void TokenBucket::refill() const {
    auto now = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(now - lastRefillTime_).count();
    tokens_ += elapsed * rate_;
    if (tokens_ > capacity_) tokens_ = capacity_;
    lastRefillTime_ = now;
}

} // namespace rpc
