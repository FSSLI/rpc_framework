// tests/test_redis_token_bucket.cc
// Redis 令牌桶测试 — 需先启动 Redis: docker run -d --name redis-test -p 6379:6379 redis:7-alpine
#ifdef HAS_REDIS

#include "rate_limiter/redis_token_bucket.h"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>

using namespace rpc;

// ============================================================================
bool testBasicAllow() {
    std::cout << "=== Test 1: Basic allow/deny ===" << std::endl;

    RedisTokenBucket bucket("127.0.0.1", 6379, 100.0, 3.0, "test_rl_basic");

    // 初始 burst 3，前 3 个通过，第 4 个拒绝
    int passed = 0;
    for (int i = 0; i < 5; ++i) {
        if (bucket.allow()) passed++;
    }
    assert(passed == 3);
    std::cout << "  Burst 3: " << passed << "/5 passed" << std::endl;
    std::cout << "  ✅ Basic allow test passed" << std::endl;
    return true;
}

// ============================================================================
bool testRateLimitAcrossMultipleClients() {
    std::cout << "=== Test 2: Multi-client rate limit ===" << std::endl;

    // 两个"客户端"共享同一个 Redis 限流器
    int passed1 = 0, passed2 = 0;
    std::thread t1([&]() {
        RedisTokenBucket b("127.0.0.1", 6379, 100.0, 5.0, "test_rl_shared");
        for (int i = 0; i < 7; ++i) if (b.allow()) passed1++;
    });
    std::thread t2([&]() {
        RedisTokenBucket b("127.0.0.1", 6379, 100.0, 5.0, "test_rl_shared");
        for (int i = 0; i < 7; ++i) if (b.allow()) passed2++;
    });
    t1.join(); t2.join();

    // 共享容量 5，14 个请求中最多 5 个通过
    int total = passed1 + passed2;
    assert(total <= 5);
    std::cout << "  Shared capacity 5: " << passed1 << " + " << passed2
              << " = " << total << " passed" << std::endl;
    std::cout << "  ✅ Multi-client rate limit test passed" << std::endl;
    return true;
}

// ============================================================================
bool testFallbackWhenRedisDown() {
    std::cout << "=== Test 3: Fallback to local when Redis unavailable ===" << std::endl;

    // 连接一个不存在的 Redis 端口，应自动 fallback 到本地
    RedisTokenBucket bucket("127.0.0.1", 16379, 100.0, 5.0, "test_rl_fallback");

    // 应该处于 fallback 模式或连接失败后自动切换
    // 本地 fallback 模式下令牌桶正常工作
    if (!bucket.isFallbackMode()) {
        std::cout << "  Redis connected (unexpected on port 16379), still testing..." << std::endl;
    }

    int passed = 0;
    for (int i = 0; i < 10; ++i) if (bucket.allow()) passed++;
    // 本地容量 5，前 5 个通过
    assert(passed == 5);
    std::cout << "  Local fallback burst 5: " << passed << "/10 passed" << std::endl;
    std::cout << "  Fallback active: " << (bucket.isFallbackMode() ? "yes" : "no") << std::endl;
    std::cout << "  ✅ Fallback test passed" << std::endl;
    return true;
}

int main() {
    std::cout << "=== RedisTokenBucket Test (requires Redis on localhost:6379) ===" << std::endl;
    bool ok = testBasicAllow() && testRateLimitAcrossMultipleClients() && testFallbackWhenRedisDown();
    std::cout << (ok ? "\nAll Redis token bucket tests passed!\n" : "\nFAILED\n");
    return ok ? 0 : 1;
}

#else
#include <iostream>
int main() { std::cout << "Redis token bucket test skipped (build with -DWITH_REDIS=ON)\n"; return 0; }
#endif
