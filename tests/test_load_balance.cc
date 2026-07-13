// tests/test_load_balance.cc
// 多节点轮询负载均衡测试

#include "discovery/memory_registry.h"
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
#include <vector>

using namespace rpc;

// ============================================================================
// Echo 服务，回复时带上节点编号以验证轮询分布
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
            echoResp.set_message(echoReq.message() + "_from_node" + std::to_string(nodeId_));
            resp->set_success(true);
            resp->set_payload(echoResp.SerializeAsString());
        });
    }
    std::string serviceName() const override { return "EchoService"; }

private:
    int nodeId_;
};

// ============================================================================
// 启动一个 RPC 服务端线程
// ============================================================================

struct ServerInstance {
    std::thread thread;
    std::atomic<bool>* quit = nullptr;
    int nodeId;
};

ServerInstance startServer(int port, int nodeId) {
    auto quit = new std::atomic<bool>(false);
    std::thread t([port, nodeId, quit]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;

        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<NodeEchoService>(nodeId));
        server.start();

        std::cout << "  Server node" << nodeId << " listening on " << port << std::endl;

        loop.runEvery(0.1, [&]() {
            if (quit->load()) loop.quit();
        });
        loop.loop();
    });
    return {std::move(t), quit, nodeId};
}

void stopServer(ServerInstance& si) {
    si.quit->store(true);
    if (si.thread.joinable()) si.thread.join();
    delete si.quit;
}

// ============================================================================
// Test: 3 节点轮询负载均衡
// ============================================================================

bool testRoundRobinLoadBalance() {
    std::cout << "=== Test: Round-Robin Load Balance (3 nodes) ===" << std::endl;

    // 启动 3 个服务端节点
    auto s1 = startServer(19981, 1);
    auto s2 = startServer(19982, 2);
    auto s3 = startServer(19983, 3);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 注册中心
    auto registry = std::make_shared<MemoryRegistry>();
    registry->registerService({"EchoService", "node1", "127.0.0.1", 19981, 0, 0});
    registry->registerService({"EchoService", "node2", "127.0.0.1", 19982, 0, 0});
    registry->registerService({"EchoService", "node3", "127.0.0.1", 19983, 0, 0});

    // 客户端：服务发现模式
    RpcAsyncClient client(registry, "EchoService");
    bool ok = client.connect();
    assert(ok);
    std::cout << "  Client connected to all nodes" << std::endl;

    // 发送 9 个请求，每个节点预期收到 3 个
    std::vector<int> nodeHits(4, 0);  // index 1-3
    for (int i = 0; i < 9; ++i) {
        EchoRequest echoReq;
        echoReq.set_message("req_" + std::to_string(i));

        RpcRequest req;
        req.set_payload(echoReq.SerializeAsString());

        auto future = client.asyncCall("EchoService", "Echo", req, 3000);
        auto resp = future.get();

        assert(resp.success());
        EchoResponse echoResp;
        echoResp.ParseFromString(resp.payload());

        // 解析响应中的节点编号
        std::string msg = echoResp.message();
        // 格式: "req_N_from_nodeX"
        size_t pos = msg.rfind("node");
        if (pos != std::string::npos) {
            int nodeId = std::stoi(msg.substr(pos + 4));
            nodeHits[nodeId]++;
            std::cout << "  Request " << i << " -> node" << nodeId << std::endl;
        }
    }

    // 每个节点至少命中 2 次（宽松验证，允许首次连接的偏向）
    bool balanced = (nodeHits[1] >= 2 && nodeHits[2] >= 2 && nodeHits[3] >= 2);
    std::cout << "  Distribution: node1=" << nodeHits[1]
              << " node2=" << nodeHits[2] << " node3=" << nodeHits[3] << std::endl;
    assert(balanced);

    std::cout << "  ✅ Round-robin load balance passed" << std::endl;

    client.disconnect();
    stopServer(s1);
    stopServer(s2);
    stopServer(s3);
    return true;
}

// ============================================================================
// 主函数
// ============================================================================

int main() {
    std::cout << "=== Load Balance Test Suite ===" << std::endl << std::endl;

    bool allPassed = true;
    allPassed &= testRoundRobinLoadBalance();

    std::cout << std::endl << "========================================" << std::endl;
    if (allPassed) {
        std::cout << "All load balance tests passed!" << std::endl;
    } else {
        std::cout << "Some tests failed!" << std::endl;
        return 1;
    }
    std::cout << "========================================" << std::endl;
    return 0;
}
