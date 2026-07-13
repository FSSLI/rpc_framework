// tests/test_connection_pool.cc
// 连接池测试：createPool / acquire 轮询 / 池模式 RpcAsyncClient

#include "pool/connection_pool.h"
#include "client/rpc_async_client.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"
#include "protocol/rpc_service.pb.h"
#include "network/event_loop.h"
#include "network/tcp_client.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>
#include <cstring>
#include <netinet/in.h>

using namespace rpc;

// ============================================================================
// 测试用的 EchoService（内联）
// ============================================================================

class PoolTestEchoService : public RpcService {
public:
    PoolTestEchoService() {
        registerMethod("Echo", [this](const RpcRequest& req, RpcResponse* resp) {
            EchoRequest echoReq;
            if (!echoReq.ParseFromString(req.payload())) {
                resp->set_success(false);
                resp->set_error_msg("parse failed");
                return;
            }
            EchoResponse echoResp;
            echoResp.set_message(echoReq.message());
            resp->set_success(true);
            resp->set_payload(echoResp.SerializeAsString());
        });
    }
    std::string serviceName() const override { return "EchoService"; }
};

// ============================================================================
// Test 1: 基本 createPool + acquire
// ============================================================================

bool testCreateAndAcquire() {
    std::cout << "=== Test 1: Create Pool & Acquire ===" << std::endl;

    // 先启动服务端
    std::atomic<bool> serverReady{false};
    std::atomic<bool> serverQuit{false};

    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(19991);
        addr.sin_addr.s_addr = INADDR_ANY;

        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<PoolTestEchoService>());
        server.start();

        serverReady = true;
        loop.runEvery(0.1, [&]() {
            if (serverQuit.load()) loop.quit();
        });
        loop.loop();
    });

    while (!serverReady.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 创建连接池
    ConnectionPool pool;
    int poolSize = 3;
    bool ok = pool.createPool("127.0.0.1", 19991, poolSize);
    assert(ok);
    assert(pool.poolSize("127.0.0.1", 19991) == poolSize);

    // acquire N 次，验证每次都能拿到非空 TcpClient
    for (int i = 0; i < poolSize * 2; ++i) {
        auto client = pool.acquire("127.0.0.1", 19991);
        assert(client != nullptr);
        auto conn = client->connection();
        assert(conn != nullptr);
        assert(conn->connected());
    }

    std::cout << "  ✅ Create & acquire passed (" << poolSize << " connections)" << std::endl;

    serverQuit = true;
    serverThread.join();
    return true;
}

// ============================================================================
// Test 2: 轮询分布验证
// ============================================================================

bool testRoundRobinDistribution() {
    std::cout << "=== Test 2: Round-Robin Distribution ===" << std::endl;

    std::atomic<bool> serverReady{false};
    std::atomic<bool> serverQuit{false};

    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(19992);
        addr.sin_addr.s_addr = INADDR_ANY;

        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<PoolTestEchoService>());
        server.start();

        serverReady = true;
        loop.runEvery(0.1, [&]() {
            if (serverQuit.load()) loop.quit();
        });
        loop.loop();
    });

    while (!serverReady.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ConnectionPool pool;
    int N = 4;
    bool ok = pool.createPool("127.0.0.1", 19992, N);
    assert(ok);

    // 记录每个位置被分配的次数
    std::vector<int> counts(N, 0);
    std::vector<ConnectionPool::TcpClientPtr> seen(N, nullptr);

    for (int i = 0; i < N; ++i) {
        auto client = pool.acquire("127.0.0.1", 19992);
        // 找到它在池中的索引
        for (int j = 0; j < N; ++j) {
            if (seen[j] == nullptr) {
                seen[j] = client;
                counts[j] = 1;
                break;
            } else if (seen[j] == client) {
                counts[j]++;
                break;
            }
        }
    }

    // 再 acquire N 次，验证轮询回到第一个
    for (int i = 0; i < N; ++i) {
        auto client = pool.acquire("127.0.0.1", 19992);
        for (int j = 0; j < N; ++j) {
            if (seen[j] == client) {
                counts[j]++;
                break;
            }
        }
    }

    // 每个客户端应该被分配了 2 次
    bool allTwo = true;
    for (int j = 0; j < N; ++j) {
        if (counts[j] != 2) {
            std::cerr << "  Client " << j << " count=" << counts[j] << " (expected 2)" << std::endl;
            allTwo = false;
        }
    }
    assert(allTwo);

    std::cout << "  ✅ Round-robin distribution passed (each of " << N
              << " clients got 2 acquires)" << std::endl;

    serverQuit = true;
    serverThread.join();
    return true;
}

// ============================================================================
// Test 3: 池模式 RpcAsyncClient 收发
// ============================================================================

bool testPooledRpcClient() {
    std::cout << "=== Test 3: Pooled RpcAsyncClient Send/Recv ===" << std::endl;

    std::atomic<bool> serverReady{false};
    std::atomic<bool> serverQuit{false};

    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(19993);
        addr.sin_addr.s_addr = INADDR_ANY;

        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<PoolTestEchoService>());
        server.start();

        serverReady = true;
        loop.runEvery(0.1, [&]() {
            if (serverQuit.load()) loop.quit();
        });
        loop.loop();
    });

    while (!serverReady.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ConnectionPool pool;
    bool ok = pool.createPool("127.0.0.1", 19993, 2);
    assert(ok);

    auto pooledClient = pool.acquire("127.0.0.1", 19993);
    assert(pooledClient != nullptr);

    // 池模式 RpcAsyncClient
    RpcAsyncClient asyncClient(pooledClient);
    bool connected = asyncClient.connect();
    assert(connected);

    // 发送 RPC 请求
    EchoRequest echoReq;
    echoReq.set_message("hello from pooled client");

    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());

    auto future = asyncClient.asyncCall("EchoService", "Echo", req, 3000);
    auto resp = future.get();

    assert(resp.success());
    EchoResponse echoResp;
    echoResp.ParseFromString(resp.payload());
    assert(echoResp.message() == "hello from pooled client");

    std::cout << "  Response: " << echoResp.message() << std::endl;
    std::cout << "  ✅ Pooled RpcAsyncClient test passed" << std::endl;

    asyncClient.disconnect();
    serverQuit = true;
    serverThread.join();
    return true;
}

// ============================================================================
// 主函数
// ============================================================================

int main() {
    std::cout << "=== Connection Pool Test Suite ===" << std::endl << std::endl;

    bool allPassed = true;
    allPassed &= testCreateAndAcquire();
    allPassed &= testRoundRobinDistribution();
    allPassed &= testPooledRpcClient();

    std::cout << std::endl << "========================================" << std::endl;
    if (allPassed) {
        std::cout << "All connection pool tests passed!" << std::endl;
    } else {
        std::cout << "Some tests failed!" << std::endl;
        return 1;
    }
    std::cout << "========================================" << std::endl;
    return 0;
}
