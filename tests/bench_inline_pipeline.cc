// tests/bench_inline_pipeline.cc — 单线程流水线压测
//
// 对比 bench_pipeline（跨线程流水线）：
//   bench_pipeline       : 每连接 1 个提交线程 + 1 个 IO 线程；每请求 1 次 eventfd（send）
//                          + 1 次 eventfd（定时器）；pending 表跨线程，要加锁
//   bench_inline_pipeline: 每连接只有 1 个线程，自己发自己收；无 eventfd、无锁、无每请求定时器
//
// 用法： ./bench_inline_pipeline [连接数] [每连接在途数] [总请求数] [服务端从Reactor数]
// 默认： 8 64 2000000 4

#include "client/inline_pipeline_client.h"
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

int main(int argc, char* argv[]) {
    const int port       = 19995;
    int conns            = (argc > 1) ? std::stoi(argv[1]) : 8;
    int window           = (argc > 2) ? std::stoi(argv[2]) : 64;
    long total           = (argc > 3) ? std::stol(argv[3]) : 2000000;
    int serverThreads    = (argc > 4) ? std::stoi(argv[4]) : 4;
    const int kTimeoutMs = 3000;

    if (conns <= 0 || window <= 0 || total <= 0) {
        std::cerr << "invalid args\n";
        return 2;
    }

    std::cout << "╔══════════════════════════════════════════════════════╗\n"
              << "║  Inline Pipeline Benchmark (单线程 + 流水线)          ║\n"
              << "║  conns=" << std::setw(3) << conns
              << "  window=" << std::setw(4) << window
              << "  inflight=" << std::setw(5) << (conns * window)
              << "  req=" << std::setw(8) << total << " ║\n"
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

    // ---------------- 客户端：每线程一个单线程流水线客户端 ----------------
    std::vector<std::vector<double>> lats(conns);
    std::atomic<long> errors{0};
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};

    std::vector<std::thread> workers;
    workers.reserve(conns);
    for (int i = 0; i < conns; ++i) {
        workers.emplace_back([&, i]() {
            long remaining = total / conns + (i < static_cast<int>(total % conns) ? 1 : 0);
            auto& lat = lats[i];
            lat.reserve(static_cast<size_t>(remaining));

            InlinePipelineClient cli("127.0.0.1", static_cast<uint16_t>(port));
            if (!cli.connect(3000)) {
                errors.fetch_add(remaining);
                ready.fetch_add(1);
                return;
            }
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();

            EchoRequest echoReq;
            echoReq.set_message("inline-pipeline");
            const std::string payload = echoReq.SerializeAsString();

            auto onDone = [&lat, &errors](const RpcResponse& resp, double us) {
                if (resp.success()) {
                    lat.push_back(us);
                } else {
                    errors.fetch_add(1);
                }
            };

            while (remaining > 0 || cli.pending() > 0) {
                // 窗口有空位就补发
                while (cli.pending() < static_cast<size_t>(window) && remaining > 0) {
                    RpcRequest req;
                    req.set_payload(payload);
                    if (!cli.submit("EchoService", "Echo", req, kTimeoutMs, onDone)) {
                        errors.fetch_add(1);
                    }
                    --remaining;
                }
                if (cli.pending() == 0) break;

                cli.pumpOnce();   // 本线程 epoll_wait + 解码 + 回调 + 超时扫描
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < conns) {
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

    std::cout << "\n  conns=" << conns << "  window=" << window
              << "  serverThreads=" << serverThreads << "\n";
    std::cout << "  Done: " << done << " / " << total
              << "  Errors: " << errors.load() << "\n";
    std::cout << "  Elapsed: " << std::fixed << std::setprecision(3) << elapsed << "s"
              << "  QPS: " << std::setprecision(0) << (done / elapsed) << "/s\n";
    std::cout << "  Latency(us) → avg: " << std::setprecision(1)
              << (done > 0 ? sum / static_cast<double>(done) : 0.0)
              << "  p50: " << pct(all, 0.50)
              << "  p99: " << pct(all, 0.99)
              << "  p999: " << pct(all, 0.999) << "\n";

    quit.store(true);
    serverThread.join();
    return (errors.load() == 0 && done == total) ? 0 : 1;
}
