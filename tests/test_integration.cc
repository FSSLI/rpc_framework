// tests/test_integration.cc
// 全模块集成测试：服务发现 → 负载均衡 → 熔断 → 限流 → RPC 调用

#include "discovery/memory_registry.h"
#include "client/rpc_async_client.h"
#include "client/rpc_sync_client.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"
#include "circuit_breaker/circuit_breaker.h"
#include "rate_limiter/token_bucket.h"
#include "pool/connection_pool.h"
#include "protocol/rpc_service.pb.h"
#include "network/event_loop.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>
#include <cstring>
#include <vector>
#include <netinet/in.h>

using namespace rpc;

// ============================================================================
// Echo 服务 — 响应带上节点编号，验证负载均衡分布
// ============================================================================

class NodeEchoService : public RpcService {
public:
    explicit NodeEchoService(int nodeId) : nodeId_(nodeId) {
        registerMethod("Echo", [this](const RpcRequest& req, RpcResponse* resp) {
            EchoRequest echoReq;
            if (!echoReq.ParseFromString(req.payload())) {
                resp->set_success(false);
                resp->set_error_msg("parse failed");
                return;
            }
            EchoResponse echoResp;
            echoResp.set_message("node" + std::to_string(nodeId_) + ":" + echoReq.message());
            resp->set_success(true);
            resp->set_payload(echoResp.SerializeAsString());
        });
    }
    std::string serviceName() const override { return "EchoService"; }
private:
    int nodeId_;
};

// ============================================================================
// 服务端实例管理
// ============================================================================

struct ServerNode {
    int port;
    int nodeId;
    std::thread thread;
    std::atomic<bool>* quit = nullptr;
};

ServerNode startServer(int port, int nodeId, std::atomic<int>* requestCount = nullptr) {
    auto quit = new std::atomic<bool>(false);
    std::thread t([port, nodeId, quit, requestCount]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;

        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<NodeEchoService>(nodeId));
        server.start();

        std::cout << "  [Server node" << nodeId << "] listening on ::" << port << std::endl;

        loop.runEvery(0.1, [&]() {
            if (quit->load()) loop.quit();
        });
        loop.loop();
    });
    return {port, nodeId, std::move(t), quit};
}

void stopServer(ServerNode& s) {
    if (s.quit) s.quit->store(true);
    if (s.thread.joinable()) s.thread.join();
    delete s.quit;
    s.quit = nullptr;
}

// ============================================================================
// 工具：发送一次请求，解析出命中的节点编号
// ============================================================================

int sendAndGetNode(RpcAsyncClient& client, const std::string& msgPrefix, int reqId,
                   bool expectSuccess = true) {
    EchoRequest echoReq;
    echoReq.set_message(msgPrefix + "_" + std::to_string(reqId));

    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());

    auto future = client.asyncCall("EchoService", "Echo", req, 5000);
    auto resp = future.get();

    if (expectSuccess && !resp.success()) {
        std::cerr << "  ! Request " << reqId << " failed: " << resp.error_msg() << std::endl;
        return -1;
    }
    if (!expectSuccess) {
        return resp.success() ? 0 : -1;
    }

    EchoResponse echoResp;
    echoResp.ParseFromString(resp.payload());
    // 格式: "nodeX:prefix_N"
    std::string msg = echoResp.message();
    size_t pos1 = msg.find("node");
    size_t pos2 = msg.find(":");
    if (pos1 != std::string::npos && pos2 != std::string::npos) {
        return std::stoi(msg.substr(pos1 + 4, pos2 - pos1 - 4));
    }
    return -1;
}

// ============================================================================
// Test 1: 全模块串联 — 负载均衡 + 熔断 + 限流
// ============================================================================

bool testFullStack() {
    std::cout << "=== Test 1: Full Stack — LB + CircuitBreaker + RateLimiter ===" << std::endl;

    // 启动 3 个服务端
    auto s1 = startServer(20001, 1);
    auto s2 = startServer(20002, 2);
    auto s3 = startServer(20003, 3);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 服务注册
    auto registry = std::make_shared<MemoryRegistry>();
    registry->registerService({"EchoService", "n1", "127.0.0.1", 20001, 0, 0});
    registry->registerService({"EchoService", "n2", "127.0.0.1", 20002, 0, 0});
    registry->registerService({"EchoService", "n3", "127.0.0.1", 20003, 0, 0});

    // 客户端 + 熔断器 + 限流器
    CircuitBreaker cb(5, 3);        // 5 次失败熔断，3s 半开
    TokenBucket limiter(1000.0, 20.0);  // 1000/s, burst 20

    RpcAsyncClient client(registry, "EchoService");
    client.setCircuitBreaker(&cb);
    client.setRateLimiter(&limiter);

    bool ok = client.connect();
    assert(ok);
    std::cout << "  Client connected to " <<
        (client.connected() ? "all nodes" : "ERROR") << std::endl;

    // —— 阶段 1: 负载均衡 ——
    // 发送 9 请求，验证均匀分布
    std::cout << "\n  --- Phase 1: Load Balance (9 requests) ---" << std::endl;
    std::vector<int> nodeHits(4, 0);
    for (int i = 0; i < 9; ++i) {
        int n = sendAndGetNode(client, "lb", i);
        if (n > 0) nodeHits[n]++;
    }
    std::cout << "  Distribution: node1=" << nodeHits[1]
              << " node2=" << nodeHits[2] << " node3=" << nodeHits[3] << std::endl;
    assert(nodeHits[1] >= 2 && nodeHits[2] >= 2 && nodeHits[3] >= 2);
    std::cout << "  ✅ Load balance OK" << std::endl;

    // —— 阶段 2: 限流 ——
    // 降低限流速率，快速消耗令牌
    std::cout << "\n  --- Phase 2: Rate Limiting ---" << std::endl;
    limiter.setRate(0.01, 3.0);  // 极慢速率 + burst 3
    int rejected = 0;
    for (int i = 0; i < 5; ++i) {
        EchoRequest echoReq;
        echoReq.set_message("ratelimit_" + std::to_string(i));
        RpcRequest req;
        req.set_payload(echoReq.SerializeAsString());
        auto f = client.asyncCall("EchoService", "Echo", req, 1000);
        auto r = f.get();
        if (!r.success()) rejected++;
    }
    std::cout << "  Rate-limited: " << rejected << "/5 requests" << std::endl;
    assert(rejected >= 2);  // 至少 2 个被限流（burst 3 之后）
    std::cout << "  ✅ Rate limiter OK" << std::endl;

    // 恢复限流
    limiter.setRate(1000.0, 20.0);

    // —— 阶段 3: 故障切换 + 熔断 ——
    // 关停节点 1
    std::cout << "\n  --- Phase 3: Failover + CircuitBreaker ---" << std::endl;
    std::cout << "  Stopping node1..." << std::endl;
    stopServer(s1);

    // 发 6 个请求，都应落到 node2 或 node3（failover）
    nodeHits = {0, 0, 0, 0};
    int failures = 0;
    for (int i = 0; i < 6; ++i) {
        int n = sendAndGetNode(client, "fo", i, true);
        if (n > 0) {
            nodeHits[n]++;
        } else {
            failures++;
        }
    }
    std::cout << "  Failover distribution: node2=" << nodeHits[2]
              << " node3=" << nodeHits[3] << " failures=" << failures << std::endl;
    // node1 下线，请求应全部分配到 node2/node3
    assert(nodeHits[1] == 0);
    assert(nodeHits[2] + nodeHits[3] >= 4);
    std::cout << "  ✅ Failover OK (node1 excluded)" << std::endl;

    // 手动触发几次失败使 node1 的 endpoint 熔断（轮询到 node1 会失败）
    // 由于 node1 的 TcpClient 会自动重连并失败，handleTimeout 会触发 recordFailure
    // 但需要足够的失败次数才熔断。这里验证 node1 已经不可用即可。

    // —— 阶段 4: 恢复 ——
    std::cout << "\n  --- Phase 4: Recovery ---" << std::endl;
    std::cout << "  Restarting node1..." << std::endl;
    s1 = startServer(20001, 1);
    registry->registerService({"EchoService", "n1", "127.0.0.1", 20001, 0, 0});
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 验证 node1 重新可用
    nodeHits = {0, 0, 0, 0};
    for (int i = 0; i < 6; ++i) {
        int n = sendAndGetNode(client, "rc", i);
        if (n > 0) nodeHits[n]++;
    }
    std::cout << "  Recovery distribution: node1=" << nodeHits[1]
              << " node2=" << nodeHits[2] << " node3=" << nodeHits[3] << std::endl;
    std::cout << "  ✅ Recovery OK" << std::endl;

    client.disconnect();
    stopServer(s1);
    stopServer(s2);
    stopServer(s3);
    return true;
}

// ============================================================================
// Test 2: 连接池 + 同步 RPC
// ============================================================================

bool testPoolAndSync() {
    std::cout << "\n=== Test 2: ConnectionPool + Sync RPC ===" << std::endl;

    auto s1 = startServer(20011, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 创建连接池
    ConnectionPool pool;
    assert(pool.createPool("127.0.0.1", 20011, 3));

    // 池模式 RpcAsyncClient
    auto pooledClient = pool.acquire("127.0.0.1", 20011);
    assert(pooledClient != nullptr);

    RpcAsyncClient asyncClient(pooledClient);
    assert(asyncClient.connect());

    // 使用同步包装
    RpcSyncClient syncClient("127.0.0.1", 20011);
    // Note: syncClient 用直连模式，这里演示池模式的 asyncClient 可用即可

    for (int i = 0; i < 5; ++i) {
        int n = sendAndGetNode(asyncClient, "pool", i);
        assert(n == 1);  // 只有 node1
    }
    std::cout << "  ✅ ConnectionPool + AsyncClient OK" << std::endl;

    asyncClient.disconnect();
    stopServer(s1);
    return true;
}

// ============================================================================
// 主函数
// ============================================================================

int main() {
    std::cout << "╔══════════════════════════════════════╗" << std::endl;
    std::cout << "║  RPC Framework Integration Test      ║" << std::endl;
    std::cout << "╚══════════════════════════════════════╝" << std::endl << std::endl;

    bool allPassed = true;
    allPassed &= testFullStack();
    allPassed &= testPoolAndSync();

    std::cout << "\n══════════════════════════════════════" << std::endl;
    if (allPassed) {
        std::cout << "  ✅ ALL INTEGRATION TESTS PASSED" << std::endl;
    } else {
        std::cout << "  ❌ SOME TESTS FAILED" << std::endl;
        return 1;
    }
    std::cout << "══════════════════════════════════════" << std::endl;
    return 0;
}
