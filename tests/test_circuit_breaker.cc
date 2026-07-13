// tests/test_circuit_breaker.cc
// 熔断器测试：CLOSED→OPEN→HALF_OPEN→CLOSED 状态机

#include "circuit_breaker/circuit_breaker.h"
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
// Test 1: 状态机基本转换
// ============================================================================

bool testStateMachine() {
    std::cout << "=== Test 1: State Machine Transitions ===" << std::endl;

    CircuitBreaker cb(3, 2);  // 3 次失败熔断，2 秒后半开

    // CLOSED: 请求应该被允许
    assert(cb.state() == CircuitBreaker::CLOSED);
    assert(cb.allowRequest());
    assert(cb.allowRequest());

    // 记录 2 次失败，仍在 CLOSED
    cb.recordFailure();
    cb.recordFailure();
    assert(cb.state() == CircuitBreaker::CLOSED);

    // 第 3 次失败，转 OPEN
    cb.recordFailure();
    assert(cb.state() == CircuitBreaker::OPEN);

    // OPEN: 请求被拒绝
    assert(!cb.allowRequest());

    // 成功记录在 OPEN 状态下应被忽略
    cb.recordSuccess();
    assert(cb.state() == CircuitBreaker::OPEN);

    // 等待 2 秒超时
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 超时后第一次 allowRequest 转 HALF_OPEN
    assert(cb.allowRequest());  // 探测请求通过
    assert(cb.state() == CircuitBreaker::HALF_OPEN);

    // HALF_OPEN 成功 → CLOSED
    cb.recordSuccess();
    assert(cb.state() == CircuitBreaker::CLOSED);
    assert(cb.failureCount() == 0);

    std::cout << "  ✅ State machine test passed" << std::endl;
    return true;
}

// ============================================================================
// Test 2: HALF_OPEN 探测失败 → 重新 OPEN
// ============================================================================

bool testHalfOpenFailure() {
    std::cout << "=== Test 2: Half-Open Probe Failure ===" << std::endl;

    CircuitBreaker cb(2, 1);  // 2 次失败熔断，1 秒半开

    // 快速触发 OPEN
    cb.recordFailure();
    cb.recordFailure();
    assert(cb.state() == CircuitBreaker::OPEN);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 进入 HALF_OPEN
    assert(cb.allowRequest());
    assert(cb.state() == CircuitBreaker::HALF_OPEN);

    // 探测失败 → 回到 OPEN
    cb.recordFailure();
    assert(cb.state() == CircuitBreaker::OPEN);

    std::cout << "  ✅ Half-open failure test passed" << std::endl;
    return true;
}

// ============================================================================
// Test 3: RpcAsyncClient 集成熔断器
// ============================================================================

class TestEchoService : public RpcService {
public:
    TestEchoService() {
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

bool testRpcIntegration() {
    std::cout << "=== Test 3: RPC Client + CircuitBreaker ===" << std::endl;

    // 启动服务端
    std::atomic<bool> serverQuit{false};
    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(19994);
        addr.sin_addr.s_addr = INADDR_ANY;
        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<TestEchoService>());
        server.start();
        loop.runEvery(0.1, [&]() { if (serverQuit.load()) loop.quit(); });
        loop.loop();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 客户端 + 熔断器
    CircuitBreaker cb(3, 2);  // 3 次失败熔断，2 秒半开
    RpcAsyncClient client("127.0.0.1", 19994);
    client.setCircuitBreaker(&cb);
    assert(client.connect());

    // 发送正常请求 → 应成功
    EchoRequest echoReq;
    echoReq.set_message("hello");
    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());

    auto f1 = client.asyncCall("EchoService", "Echo", req, 3000);
    assert(f1.get().success());
    assert(cb.state() == CircuitBreaker::CLOSED);

    // 模拟故障：请求一个不存在的服务 → 失败
    auto f2 = client.asyncCall("NoSuchService", "NoSuchMethod", req, 3000);
    assert(!f2.get().success());  // 服务不存在
    cb.recordFailure();
    cb.recordFailure();
    cb.recordFailure();
    assert(cb.state() == CircuitBreaker::OPEN);

    // 熔断后请求被直接拒绝
    auto f3 = client.asyncCall("EchoService", "Echo", req, 3000);
    auto r3 = f3.get();
    assert(!r3.success());
    std::cout << "  Circuit open response: " << r3.error_msg() << std::endl;

    // 等待半开
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 半开后探测请求通过
    auto f4 = client.asyncCall("EchoService", "Echo", req, 3000);
    assert(f4.get().success());
    assert(cb.state() == CircuitBreaker::CLOSED);

    std::cout << "  ✅ RPC integration test passed" << std::endl;

    client.disconnect();
    serverQuit = true;
    serverThread.join();
    return true;
}

// ============================================================================
// 主函数
// ============================================================================

int main() {
    std::cout << "=== Circuit Breaker Test Suite ===" << std::endl << std::endl;

    bool allPassed = true;
    allPassed &= testStateMachine();
    allPassed &= testHalfOpenFailure();
    allPassed &= testRpcIntegration();

    std::cout << std::endl << "========================================" << std::endl;
    if (allPassed) {
        std::cout << "All circuit breaker tests passed!" << std::endl;
    } else {
        std::cout << "Some tests failed!" << std::endl;
        return 1;
    }
    std::cout << "========================================" << std::endl;
    return 0;
}
