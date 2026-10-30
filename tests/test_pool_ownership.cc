// tests/test_pool_ownership.cc — TcpClient 所有权回归测试
//
// 背景：`ConnectionPool::acquire` 在线程数 > 连接数时会轮询返回同一个 TcpClient，
// 而两个 RpcAsyncClient 共享它必然互相覆盖 connection/message 回调，
// 表现为"响应投递给错误的客户端 → 请求匹配不到 → 超时"（实测 8 线程/4 连接时 4 个错误、卡 3 秒）。
//
// 修复后约定：一个 TcpClient 同一时刻只能被一个 RpcAsyncClient 绑定；
// 第二个客户端构造时声明失败，connect() 返回 false，调用立即返回错误（而不是串包或挂起）。

#include "client/rpc_async_client.h"
#include "network/event_loop_thread.h"
#include "network/tcp_client.h"

#include <arpa/inet.h>
#include <atomic>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>

using namespace rpc;

static int g_failed = 0;

static void check(const std::string& name, bool ok) {
    if (ok) {
        std::cout << "  PASS  " << name << std::endl;
    } else {
        std::cout << "  FAIL  " << name << std::endl;
        ++g_failed;
    }
}

int main() {
    std::cout << "=== TcpClient Ownership Test ===" << std::endl;

    // 不需要真实服务端：只验证"所有权声明"这一层逻辑
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    EventLoopThread loopThread;
    EventLoop* loop = loopThread.startLoop();
    auto tc = std::make_shared<TcpClient>(loop, addr);

    {
        RpcAsyncClient a(tc);
        check("第一个客户端成功声明所有权", tc->isClaimed());

        {
            RpcAsyncClient b(tc);
            check("第二个客户端 connect() 返回 false", !b.connect());

            std::atomic<bool> called{false};
            RpcResponse got;
            b.asyncCall("EchoService", "Echo", RpcRequest(),
                        [&](const RpcResponse& r) {
                            got.CopyFrom(r);
                            called.store(true);
                        },
                        1000);
            check("第二个客户端的回调被立即调用（不阻塞）", called.load());
            check("错误信息说明是所有权冲突",
                  got.error_msg().find("owned") != std::string::npos);
        }

        check("第二个客户端析构后，第一个客户端仍持有所有权", tc->isClaimed());
    }

    check("第一个客户端析构后，所有权已释放", !tc->isClaimed());

    std::cout << (g_failed == 0 ? "\n✅ Ownership test passed\n" : "\n❌ Ownership test FAILED\n");
    return g_failed == 0 ? 0 : 1;
}
