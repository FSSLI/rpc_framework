// tests/test_pool_reuse.cc — 连接池复用回归测试
//
// 验证改造后的语义：
//   1. RpcAsyncClient 析构后，池里的 TCP 连接仍然存活（不再被 disconnectPermanently 关掉）；
//   2. 下一个客户端从池里借到的是同一条 TcpClient，且可直接收发；
//   3. 显式 release() 之后，连接依然可用。
//
// 改造前：第一个客户端析构就会关掉连接，池"越用越少"，第二个客户端 connect() 直接失败。

#include "client/rpc_async_client.h"
#include "network/event_loop.h"
#include "network/tcp_client.h"
#include "network/tcp_connection.h"
#include "pool/connection_pool.h"
#include "protocol/rpc_service.pb.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"

#include <atomic>
#include <cstring>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <thread>

using namespace rpc;

static int g_failed = 0;

static void check(const std::string& name, bool ok) {
    std::cout << (ok ? "  PASS  " : "  FAIL  ") << name << std::endl;
    if (!ok) ++g_failed;
}

class PoolReuseEchoService : public RpcService {
public:
    PoolReuseEchoService() {
        registerMethod("Echo", [](const RpcRequest& req, RpcResponse* resp) {
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

static bool callEcho(RpcAsyncClient& cli, const std::string& msg) {
    EchoRequest echoReq;
    echoReq.set_message(msg);
    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());

    auto fut = cli.asyncCall("EchoService", "Echo", req, 2000);
    RpcResponse resp = fut.get();
    return resp.success();
}

int main() {
    const int port = 19994;
    std::cout << "=== Pool Reuse Test ===" << std::endl;

    // ---- 服务端 ----
    std::atomic<bool> ready{false};
    std::atomic<bool> quit{false};
    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        addr.sin_addr.s_addr = INADDR_ANY;

        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<PoolReuseEchoService>());
        server.start();
        ready.store(true);
        loop.runEvery(0.1, [&]() { if (quit.load()) loop.quit(); });
        loop.loop();
    });
    while (!ready.load()) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    ConnectionPool pool;
    if (!pool.createPool("127.0.0.1", static_cast<uint16_t>(port), 1)) {
        std::cerr << "pool creation failed" << std::endl;
        quit.store(true);
        serverThread.join();
        return 1;
    }

    std::shared_ptr<TcpClient> borrowed;

    // ---- 第一次借用 ----
    {
        borrowed = pool.acquire("127.0.0.1", static_cast<uint16_t>(port));
        check("acquire 返回非空", borrowed != nullptr);
        if (!borrowed) { quit.store(true); serverThread.join(); return 1; }

        RpcAsyncClient cli(borrowed);
        check("第一个客户端 connect 成功", cli.connect());
        check("第一个客户端收发正常", callEcho(cli, "first"));
        // cli 在这里析构 → 归还连接
    }

    // ---- 关键断言：连接没有被关掉 ----
    {
        auto conn = borrowed->connection();
        check("客户端析构后，池连接仍然存活", conn != nullptr && conn->connected());
        check("客户端析构后，所有权已释放", !borrowed->isClaimed());
    }

    // ---- 第二次借用：必须复用同一条连接 ----
    {
        auto again = pool.acquire("127.0.0.1", static_cast<uint16_t>(port));
        check("第二次 borrow 拿到同一条 TcpClient", again.get() == borrowed.get());

        RpcAsyncClient cli2(again);
        check("第二个客户端 connect 成功（复用已有连接）", cli2.connect());
        check("第二个客户端收发正常", callEcho(cli2, "second"));
        // cli2 析构 → 再次归还
    }

    // ---- 显式 release 后仍可用 ----
    {
        pool.release(borrowed);
        auto conn = borrowed->connection();
        check("显式 release 后连接仍存活", conn != nullptr && conn->connected());

        RpcAsyncClient cli3(borrowed);
        check("release 后第三次借用可收发", cli3.connect() && callEcho(cli3, "third"));
    }

    std::cout << (g_failed == 0 ? "\n✅ Pool reuse test passed\n" : "\n❌ Pool reuse test FAILED\n");

    quit.store(true);
    serverThread.join();
    return g_failed == 0 ? 0 : 1;
}
