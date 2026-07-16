// src/rate_limiter/token_bucket.h
#ifndef TOKEN_BUCKET_H
#define TOKEN_BUCKET_H

#include <mutex>
#include <chrono>

namespace rpc {

class TokenBucket {
public:
    // rate: 每秒生成的令牌数
    // capacity: 桶的最大容量（允许的突发流量）
    TokenBucket(double rate, double capacity);
    virtual ~TokenBucket() = default;

    // 尝试消费 1 个令牌，返回 true 表示通过
    virtual bool allow();

    // 动态调整速率（capacity<0 表示保持当前容量不变）
    virtual void setRate(double rate, double capacity = -1.0);

    // 查询当前令牌数
    double availableTokens() const;

protected:
    double rate_;       // 令牌/秒（子类可访问）
    double capacity_;   // 最大令牌数

private:
    void refill() const;  // 逻辑 const，更新 mutable 成员
    mutable double tokens_;     // 当前令牌数
    mutable std::mutex mutex_;
    mutable std::chrono::steady_clock::time_point lastRefillTime_;
};

} // namespace rpc

#endif
