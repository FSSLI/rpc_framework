// src/rate_limiter/redis_token_bucket.cc
#ifdef HAS_REDIS

#include "rate_limiter/redis_token_bucket.h"
#include <hiredis/hiredis.h>
#include <iostream>
#include <cstring>
#include <sstream>

namespace rpc {

// ============================================================================
// Redis Lua 脚本：原子化令牌检查 + 消费 + 惰性填充
// ============================================================================
const char* RedisTokenBucket::luaScript() {
    return R"SCRIPT(
        local tokens_key = KEYS[1]
        local timestamp_key = KEYS[2]
        local rate = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])

        local last_tokens = tonumber(redis.call('get', tokens_key))
        if last_tokens == nil then last_tokens = capacity end
        local last_refill = tonumber(redis.call('get', timestamp_key))
        if last_refill == nil then last_refill = now end

        local elapsed = math.max(0, now - last_refill)
        local new_tokens = math.min(capacity, last_tokens + elapsed * rate / 1000.0)

        if new_tokens >= 1 then
            redis.call('set', tokens_key, new_tokens - 1)
            redis.call('set', timestamp_key, now)
            return 1
        end
        redis.call('set', tokens_key, new_tokens)
        redis.call('set', timestamp_key, now)
        return 0
    )SCRIPT";
}

// ============================================================================

RedisTokenBucket::RedisTokenBucket(const std::string& redisHost, int redisPort,
                                     double rate, double capacity,
                                     const std::string& keyPrefix)
    : TokenBucket(rate, capacity),
      redisHost_(redisHost), redisPort_(redisPort),
      keyPrefix_(keyPrefix),
      localRate_(rate), localCapacity_(capacity),
      localTokens_(capacity),
      localLastRefill_(std::chrono::steady_clock::now()) {
    connectRedis();
    if (redisCtx_) {
        // 预加载 Lua 脚本，获取 SHA
        auto* reply = (redisReply*)redisCommand(redisCtx_, "SCRIPT LOAD %s", luaScript());
        if (reply && reply->type == REDIS_REPLY_STRING) {
            luaSha_ = reply->str;
        }
        if (reply) freeReplyObject(reply);
    }
}

RedisTokenBucket::~RedisTokenBucket() {
    if (redisCtx_) redisFree(redisCtx_);
}

void RedisTokenBucket::connectRedis() {
    redisCtx_ = redisConnect(redisHost_.c_str(), redisPort_);
    if (!redisCtx_ || redisCtx_->err) {
        std::cerr << "RedisTokenBucket: Redis connection failed, using local fallback" << std::endl;
        if (redisCtx_) { redisFree(redisCtx_); redisCtx_ = nullptr; }
        fallback_ = true;
    }
}

bool RedisTokenBucket::allow() {
    // Redis 降级模式下直接走本地
    if (fallback_.load() || !redisCtx_) {
        std::lock_guard<std::mutex> lock(localMutex_);
        localRefill();
        if (localTokens_ >= 1.0) { localTokens_ -= 1.0; return true; }
        return false;
    }

    auto now = std::chrono::steady_clock::now();
    auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    // 尝试 EVALSHA
    auto* reply = (redisReply*)redisCommand(redisCtx_,
        "EVALSHA %s 2 %s:tokens %s:ts %f %f %lld",
        luaSha_.c_str(),
        keyPrefix_.c_str(), keyPrefix_.c_str(),
        rate_, capacity_, (long long)nowMs);

    if (!reply) {
        // Redis 断开 → 切换到本地降级
        std::cerr << "RedisTokenBucket: Redis disconnected, switching to local fallback" << std::endl;
        fallback_ = true;
        if (redisCtx_) { redisFree(redisCtx_); redisCtx_ = nullptr; }
        return allow();  // 递归走本地路径
    }

    // NOSCRIPT 错误 → 重新 SCRIPT LOAD
    if (reply->type == REDIS_REPLY_ERROR && strstr(reply->str, "NOSCRIPT")) {
        freeReplyObject(reply);
        auto* loadReply = (redisReply*)redisCommand(redisCtx_, "SCRIPT LOAD %s", luaScript());
        if (loadReply && loadReply->type == REDIS_REPLY_STRING) {
            luaSha_ = loadReply->str;
        }
        if (loadReply) freeReplyObject(loadReply);
        return allow();  // 重试
    }

    bool result = (reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
    freeReplyObject(reply);
    return result;
}

void RedisTokenBucket::setRate(double rate, double capacity) {
    TokenBucket::setRate(rate, capacity);
    if (capacity >= 0) localCapacity_ = capacity;
    localRate_ = rate;
}

bool RedisTokenBucket::executeLua(double rate, double capacity, long long nowMs) {
    // 降级时使用本地 refill
    (void)rate; (void)capacity; (void)nowMs;
    return allow();
}

// ============================================================================
// 本地 fallback refill（逻辑 const，更新 mutable 成员）
// ============================================================================
void RedisTokenBucket::localRefill() const {
    auto now = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(now - localLastRefill_).count();
    localTokens_ += elapsed * localRate_;
    if (localTokens_ > localCapacity_) localTokens_ = localCapacity_;
    localLastRefill_ = now;
}

} // namespace rpc

#endif // HAS_REDIS
