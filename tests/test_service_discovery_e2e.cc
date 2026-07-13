// tests/test_service_discovery_e2e.cc
#include "discovery/memory_registry.h"
#include "client/rpc_async_client.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"
#include "protocol/rpc_service.pb.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>

using namespace rpc;

// 内联测试用的 EchoService
class TestEchoService : public RpcService {
public:
    TestEchoService() {
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

int main() {
    std::cout << "=== Service Discovery E2E Test ===" << std::endl;

    // 1. 启动服务端
    std::thread serverThread([]() {
        EventLoop loop;
        
        struct sockaddr_in listenAddr;
        memset(&listenAddr, 0, sizeof(listenAddr));
        listenAddr.sin_family = AF_INET;
        listenAddr.sin_port = htons(8888);
        listenAddr.sin_addr.s_addr = INADDR_ANY;
        
        RpcServer server(&loop, listenAddr);
        server.setIdleTimeout(60);
        
        auto echoService = std::make_shared<TestEchoService>();
        server.registerService(echoService);
        
        server.start();
        std::cout << "Server listening on 8888" << std::endl;
        
        loop.loop();
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 2. 创建注册中心，注册服务节点
    auto registry = std::make_shared<MemoryRegistry>();
    
    RegistryNode node{"EchoService", "node_1", "127.0.0.1", 8888, 0, 0};
    registry->registerService(node);
    
    // 3. 客户端通过服务发现连接
    RpcAsyncClient client(registry, "EchoService");
    
    if (!client.connect()) {
        std::cerr << "❌ Client connect failed" << std::endl;
        return 1;
    }
    
    std::cout << "✅ Client connected via service discovery" << std::endl;

    // 4. 发送 RPC 请求
    EchoRequest echoReq;
    echoReq.set_message("hello service discovery");
    
    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());
    
    auto future = client.asyncCall("EchoService", "Echo", req);
    
    // 5. 验证响应
    auto resp = future.get();
    if (resp.success()) {
        EchoResponse echoResp;
        if (echoResp.ParseFromString(resp.payload())) {
            if (echoResp.message() == "hello service discovery") {
                std::cout << "✅ E2E test passed! Response: " << echoResp.message() << std::endl;
            } else {
                std::cerr << "❌ Wrong response: " << echoResp.message() << std::endl;
                return 1;
            }
        } else {
            std::cerr << "❌ Parse response failed" << std::endl;
            return 1;
        }
    } else {
        std::cerr << "❌ RPC failed: " << resp.error_msg() << std::endl;
        return 1;
    }

    // 清理
    client.disconnect();
    serverThread.detach();
    
    std::cout << "\n=== E2E Test Completed ===" << std::endl;
    return 0;
}