// src/rate_limiter/redis_token_bucket.h
// 基于 Redis Lua 脚本的分布式令牌桶限流器
// 继承 TokenBucket 接口，fallback 到本地实现
// 依赖: hiredis (Redis C client)
// 编译: cmake .. -DWITH_REDIS=ON

#ifndef REDIS_TOKEN_BUCKET_H
#define REDIS_TOKEN_BUCKET_H

#ifdef HAS_REDIS

#include "rate_limiter/token_bucket.h"
#include <string>
#include <memory>
#include <atomic>

struct redisContext;

namespace rpc {

class RedisTokenBucket : public TokenBucket {
public:
    // redisHost/port: Redis 连接信息
    // keyPrefix: Redis key 前缀
    // rate/capacity: 同 TokenBucket
    RedisTokenBucket(const std::string& redisHost, int redisPort,
                     double rate, double capacity,
                     const std::string& keyPrefix = "rpc_rl");
    ~RedisTokenBucket() override;

    bool allow() override;
    void setRate(double rate, double capacity = -1.0) override;

    // 本地 fallback 是否激活
    bool isFallbackMode() const { return fallback_.load(); }

private:
    static const char* luaScript();

    bool executeLua(double rate, double capacity, long long nowMs);
    void connectRedis();

    std::string redisHost_;
    int redisPort_;
    std::string keyPrefix_;
    redisContext* redisCtx_ = nullptr;
    std::string luaSha_;  // SCRIPT LOAD 缓存的 SHA

    // 降级：Redis 不可用时自动 fallback 到本地 TokenBucket
    double localRate_;
    double localCapacity_;
    mutable std::mutex localMutex_;
    mutable double localTokens_;
    mutable std::chrono::steady_clock::time_point localLastRefill_;
    std::atomic<bool> fallback_{false};
    void localRefill() const;
};

} // namespace rpc

#else
// 无 Redis 依赖时提供空实现声明
#endif // HAS_REDIS
#endif // REDIS_TOKEN_BUCKET_H
