// tests/bench_inline_sync.cc — "调用线程驱动 IO"的同步客户端压测
//
// 对照组：test_benchmark 的 Pool 组（RpcAsyncClient + future.get()，跨线程投递）
//   8 线程 × 8 连接 = 61,683 QPS / avg 128 µs / p99 461 µs（WSL2）
//
// 本程序用 InlineSyncClient：每个工作线程自己持有 EventLoop，
// 发送直接 write、等待直接 epoll_wait、响应当场解码，
// 每次调用少了 2 次 eventfd 写 + 4 次 futex（系统调用 14 → ~5）。
//
// 用法： ./bench_inline_sync [线程数] [总请求数] [服务端从Reactor数]
// 默认： 8 50000 4

#include "client/inline_sync_client.h"
#include "network/event_loop.h"
#include "protocol/rpc_service.pb.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <thread>
#include <vector>

using namespace rpc;
using Clock = std::chrono::steady_clock;

// ============================================================================
class BenchEchoService : public RpcService {
public:
    BenchEchoService() {
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

static double pct(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0;
    size_t i = static_cast<size_t>(p * (sorted.size() - 1));
    return sorted[i];
}

// ============================================================================
int main(int argc, char* argv[]) {
    const int port        = 19992;
    int threads           = (argc > 1) ? std::stoi(argv[1]) : 8;
    long total            = (argc > 2) ? std::stol(argv[2]) : 50000;
    int serverThreads     = (argc > 3) ? std::stoi(argv[3]) : 4;
    const int kTimeoutMs  = 3000;

    if (threads <= 0 || total <= 0) {
        std::cerr << "invalid args\n";
        return 2;
    }

    std::cout << "╔══════════════════════════════════════════════════════╗\n"
              << "║  Inline Sync Benchmark (调用线程驱动 IO)              ║\n"
              << "║  threads=" << std::setw(3) << threads
              << "  req=" << std::setw(7) << total
              << "  serverThreads=" << std::setw(2) << serverThreads << "              ║\n"
              << "╚══════════════════════════════════════════════════════╝" << std::endl;

    // ---------------- 服务端 ----------------
    std::atomic<bool> serverReady{false};
    std::atomic<bool> quit{false};
    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        addr.sin_addr.s_addr = INADDR_ANY;

        RpcServer server(&loop, addr);
        if (serverThreads > 0) server.setThreadNum(serverThreads);
        server.registerService(std::make_shared<BenchEchoService>());
        server.start();
        serverReady.store(true);
        loop.runEvery(0.2, [&]() { if (quit.load()) loop.quit(); });
        loop.loop();
    });
    while (!serverReady.load()) std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // ---------------- 客户端：每线程一个 InlineSyncClient ----------------
    std::vector<std::vector<double>> lats(threads);
    std::atomic<long> errors{0};
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};

    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int i = 0; i < threads; ++i) {
        workers.emplace_back([&, i]() {
            long n = total / threads + (i < static_cast<int>(total % threads) ? 1 : 0);
            lats[i].reserve(static_cast<size_t>(n));

            InlineSyncClient cli("127.0.0.1", static_cast<uint16_t>(port));
            if (!cli.connect(3000)) {
                errors.fetch_add(n);
                ready.fetch_add(1);
                return;
            }
            ready.fetch_add(1);

            // 等所有线程都连上，再开始计时
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();

            EchoRequest echoReq;
            echoReq.set_message("inline");
            std::string payload = echoReq.SerializeAsString();

            for (long k = 0; k < n; ++k) {
                RpcRequest req;
                req.set_payload(payload);
                const auto t0 = Clock::now();
                RpcResponse resp = cli.call("EchoService", "Echo", req, kTimeoutMs);
                const double us =
                    std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
                if (!resp.success()) {
                    errors.fetch_add(1);
                } else {
                    lats[i].push_back(us);
                }
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto tStart = Clock::now();
    go.store(true, std::memory_order_release);

    for (auto& t : workers) t.join();
    const auto tEnd = Clock::now();
    const double elapsed = std::chrono::duration<double>(tEnd - tStart).count();

    // ---------------- 汇总 ----------------
    std::vector<double> all;
    all.reserve(static_cast<size_t>(total));
    for (auto& v : lats) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());

    double sum = 0;
    for (double v : all) sum += v;
    const long done = static_cast<long>(all.size());

    std::cout << "\n  threads=" << threads << "  serverThreads=" << serverThreads << "\n";
    std::cout << "  Done: " << done << " / " << total << "  Errors: " << errors.load() << "\n";
    std::cout << "  Elapsed: " << std::fixed << std::setprecision(3) << elapsed << "s"
              << "  QPS: " << std::setprecision(0) << (done / elapsed) << "/s\n";
    std::cout << "  Latency(us) → avg: " << std::setprecision(1)
              << (done > 0 ? sum / static_cast<double>(done) : 0.0)
              << "  p50: " << pct(all, 0.50)
              << "  p99: " << pct(all, 0.99)
              << "  p999: " << pct(all, 0.999) << "\n";

    quit.store(true);
    serverThread.join();
    return (errors.load() == 0) ? 0 : 1;
}
