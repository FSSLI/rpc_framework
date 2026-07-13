// tests/test_token_bucket.cc
// 令牌桶限流测试

#include "rate_limiter/token_bucket.h"
#include "client/rpc_async_client.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"
#include "protocol/rpc_service.pb.h"
#include "network/event_loop.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>
#include <cstring>
#include <netinet/in.h>

using namespace rpc;

// ============================================================================
// Test 1: 基本令牌消耗和速率限制
// ============================================================================

bool testBasicRateLimit() {
    std::cout << "=== Test 1: Basic Rate Limiting ===" << std::endl;

    TokenBucket bucket(100.0, 10.0);  // 100/s, 最大突发 10

    // 初始有满桶令牌，前 10 个应该通过
    int passed = 0;
    for (int i = 0; i < 15; ++i) {
        if (bucket.allow()) passed++;
    }
    assert(passed == 10);  // 容量 10，最多 10 个通过
    std::cout << "  Burst: " << passed << "/15 passed (capacity=10)" << std::endl;

    // 等 0.1 秒补充 ~10 个令牌
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    passed = 0;
    for (int i = 0; i < 15; ++i) {
        if (bucket.allow()) passed++;
    }
    assert(passed >= 8 && passed <= 12);  // 100/s * 0.1s = 10 令牌
    std::cout << "  After 100ms: " << passed << "/15 passed (expected ~10)" << std::endl;

    std::cout << "  ✅ Basic rate limit test passed" << std::endl;
    return true;
}

// ============================================================================
// Test 2: 动态调整速率
// ============================================================================

bool testDynamicRate() {
    std::cout << "=== Test 2: Dynamic Rate Adjustment ===" << std::endl;

    TokenBucket bucket(10.0, 1.0);  // 极慢速率

    // 初始 1 个通过
    assert(bucket.allow());
    assert(!bucket.allow());  // 令牌耗尽

    // 调高速率和容量
    bucket.setRate(1000.0, 100.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // 等 10ms → ~10 tokens
    int passed = 0;
    for (int i = 0; i < 15; ++i) {
        if (bucket.allow()) passed++;
    }
    assert(passed >= 5);
    std::cout << "  After rate increase: " << passed << "/15 passed" << std::endl;

    std::cout << "  ✅ Dynamic rate test passed" << std::endl;
    return true;
}

// ============================================================================
// Test 3: RpcAsyncClient 集成限流
// ============================================================================

class LimiterEchoService : public RpcService {
public:
    LimiterEchoService() {
        registerMethod("Echo", [this](const RpcRequest& req, RpcResponse* resp) {
            EchoRequest echoReq;
            echoReq.ParseFromString(req.payload());
            EchoResponse echoResp;
            echoResp.set_message(echoReq.message());
            resp->set_success(true);
            resp->set_payload(echoResp.SerializeAsString());
        });
    }
    std::string serviceName() const override { return "EchoService"; }
};

bool testRpcRateLimit() {
    std::cout << "=== Test 3: RPC Client + Rate Limiter ===" << std::endl;

    std::atomic<bool> serverQuit{false};
    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(19995);
        addr.sin_addr.s_addr = INADDR_ANY;
        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<LimiterEchoService>());
        server.start();
        loop.runEvery(0.1, [&]() { if (serverQuit.load()) loop.quit(); });
        loop.loop();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 极慢速率(0.01/s)确保只有初始容量决定结果，不受网络延迟干扰
    TokenBucket limiter(0.01, 3.0);
    RpcAsyncClient client("127.0.0.1", 19995);
    client.setRateLimiter(&limiter);
    assert(client.connect());

    EchoRequest echoReq;
    echoReq.set_message("hi");
    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());

    // 并发发送 4 个请求（不等待响应，避免令牌桶在等待期间补充）
    auto f0 = client.asyncCall("EchoService", "Echo", req, 3000);
    auto f1 = client.asyncCall("EchoService", "Echo", req, 3000);
    auto f2 = client.asyncCall("EchoService", "Echo", req, 3000);
    auto f3 = client.asyncCall("EchoService", "Echo", req, 3000);

    int ok = 0, limited = 0;
    auto check = [&](auto& f) {
        auto r = f.get();
        if (r.success()) ok++;
        else {
            std::cout << "  Rejected: " << r.error_msg() << std::endl;
            limited++;
        }
    };
    check(f0); check(f1); check(f2); check(f3);

    assert(ok == 3 && limited == 1);  // 容量 3 + 速率极慢 → 第 4 个被限流
    std::cout << "  Passed: " << ok << ", Rate-limited: " << limited << std::endl;
    std::cout << "  ✅ RPC rate limit test passed" << std::endl;

    client.disconnect();
    serverQuit = true;
    serverThread.join();
    return true;
}

// ============================================================================
// 主函数
// ============================================================================

int main() {
    std::cout << "=== Token Bucket Test Suite ===" << std::endl << std::endl;

    bool allPassed = true;
    allPassed &= testBasicRateLimit();
    allPassed &= testDynamicRate();
    allPassed &= testRpcRateLimit();

    std::cout << std::endl << "========================================" << std::endl;
    if (allPassed) {
        std::cout << "All token bucket tests passed!" << std::endl;
    } else {
        std::cout << "Some tests failed!" << std::endl;
        return 1;
    }
    std::cout << "========================================" << std::endl;
    return 0;
}
