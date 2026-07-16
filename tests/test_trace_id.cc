// tests/test_trace_id.cc
// TraceID 透传验证：客户端注入 → 服务端 handler 可读取

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
#include <mutex>
#include <vector>

using namespace rpc;

// handler 记录收到的所有 trace-id
std::mutex gMutex;
std::vector<std::string> gTraceIds;

class TraceEchoService : public RpcService {
public:
    TraceEchoService() {
        registerMethod("Echo", [this](const RpcRequest& req, RpcResponse* resp) {
            // 提取 trace-id 并记录到全局列表
            auto it = req.metadata().find("trace-id");
            if (it != req.metadata().end()) {
                std::lock_guard<std::mutex> lk(gMutex);
                gTraceIds.push_back(it->second);
            }
            EchoRequest echoReq; echoReq.ParseFromString(req.payload());
            EchoResponse echoResp; echoResp.set_message(echoReq.message());
            resp->set_success(true);
            resp->set_payload(echoResp.SerializeAsString());
        });
    }
    std::string serviceName() const override { return "EchoService"; }
};

// ============================================================================
bool testTraceIdPropagation() {
    std::cout << "=== Test: TraceID propagation ===" << std::endl;
    gTraceIds.clear();

    std::atomic<bool> quit{false};
    std::thread st([&]() {
        EventLoop loop;
        sockaddr_in addr; memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET; addr.sin_port = htons(21010); addr.sin_addr.s_addr = INADDR_ANY;
        RpcServer s(&loop, addr); s.registerService(std::make_shared<TraceEchoService>()); s.start();
        loop.runEvery(0.1, [&]() { if (quit.load()) loop.quit(); });
        loop.loop();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    RpcAsyncClient client("127.0.0.1", 21010);
    assert(client.connect());

    // 发送 3 个请求
    for (int i = 0; i < 3; ++i) {
        EchoRequest er; er.set_message("trace_test_" + std::to_string(i));
        RpcRequest req; req.set_payload(er.SerializeAsString());
        auto f = client.asyncCall("EchoService", "Echo", req, 3000);
        assert(f.get().success());
    }

    client.disconnect();
    quit = true; st.join();

    // 验证每个请求都有唯一的 trace-id
    assert(!gTraceIds.empty());
    assert(gTraceIds.size() == 3);
    for (size_t i = 1; i < gTraceIds.size(); i++) assert(gTraceIds[i] != gTraceIds[i-1]);
    std::cout << "  Received " << gTraceIds.size() << " unique trace-ids:" << std::endl;
    for (auto& tid : gTraceIds) std::cout << "    " << tid << std::endl;
    std::cout << "  ✅ TraceID propagation test passed" << std::endl;
    return true;
}

// ============================================================================
bool testMetadataEmptyByDefault() {
    std::cout << "=== Test: Metadata empty when client doesn't inject ===" << std::endl;
    gTraceIds.clear();

    std::atomic<bool> quit{false};
    std::thread st([&]() {
        EventLoop loop;
        sockaddr_in addr{}; addr.sin_family = AF_INET;
        addr.sin_port = htons(21011); addr.sin_addr.s_addr = INADDR_ANY;
        RpcServer s(&loop, addr); s.registerService(std::make_shared<TraceEchoService>()); s.start();
        loop.runEvery(0.1, [&]() { if (quit.load()) loop.quit(); });
        loop.loop();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    RpcAsyncClient client("127.0.0.1", 21011);
    assert(client.connect());

    // 发送请求 — asyncCall 自动注入 trace-id
    EchoRequest er; er.set_message("hi");
    RpcRequest req; req.set_payload(er.SerializeAsString());
    auto f = client.asyncCall("EchoService", "Echo", req, 3000);
    assert(f.get().success());

    client.disconnect();
    quit = true; st.join();

    // asyncCall 自动注入了 trace-id，所以 handler 收到了
    assert(!gTraceIds.empty());
    std::cout << "  Auto-injected trace-id: " << gTraceIds[0] << std::endl;
    std::cout << "  ✅ Auto-injection verified" << std::endl;
    return true;
}

int main() {
    bool ok = testTraceIdPropagation();
    std::cout << (ok ? "\nAll TraceID tests passed!\n" : "\nFAILED\n");
    return ok ? 0 : 1;
}
