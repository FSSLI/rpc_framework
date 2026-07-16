// tests/test_benchmark.cc — RPC 框架压测 + 性能数据采集
// 用法: ./test_benchmark [总请求数] [并发线程数]
// 默认: 10000 请求, 4 线程

#include "client/rpc_async_client.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"
#include "pool/connection_pool.h"
#include "protocol/rpc_service.pb.h"
#include "network/event_loop.h"
#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <mutex>
#include <netinet/in.h>

using namespace rpc;

// ============================================================================
class BenchEchoService : public RpcService {
public:
    BenchEchoService() {
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

// ============================================================================
struct Stats {
    std::vector<double> us;  // latency in microseconds
    size_t errors = 0;
    double avg() const { return us.empty() ? 0 : std::accumulate(us.begin(), us.end(), 0.0) / us.size(); }
    double p50() const { return pct(0.50); }
    double p99() const { return pct(0.99); }
    double p999() const { return pct(0.999); }
private:
    double pct(double p) const {
        if (us.empty()) return 0;
        auto s = us; std::sort(s.begin(), s.end());
        size_t i = static_cast<size_t>(p * s.size());
        return s[std::min(i, s.size() - 1)];
    }
};

// ============================================================================
double timedCall(RpcAsyncClient& client, const std::string& msg, int timeoutMs = 5000) {
    auto t0 = std::chrono::steady_clock::now();
    EchoRequest echoReq; echoReq.set_message(msg);
    RpcRequest req; req.set_payload(echoReq.SerializeAsString());
    auto future = client.asyncCall("EchoService", "Echo", req, timeoutMs);
    auto resp = future.get();
    auto t1 = std::chrono::steady_clock::now();
    return resp.success()
        ? std::chrono::duration<double, std::micro>(t1 - t0).count()
        : -1.0;
}

void printStats(const std::string& label, const Stats& s, double elapsed) {
    std::cout << "\n  ── " << label << " ──" << std::endl;
    std::cout << "  Requests: " << s.us.size() + s.errors
              << "  Errors: " << s.errors
              << "  Elapsed: " << std::fixed << std::setprecision(2) << elapsed << "s"
              << "  QPS: " << std::setprecision(0) << ((s.us.size() + s.errors) / elapsed) << "/s" << std::endl;
    std::cout << "  Latency → avg: " << std::setprecision(0) << s.avg()
              << " us  p50: " << s.p50()
              << " us  p99: " << s.p99()
              << " us  p999: " << s.p999() << " us" << std::endl;
}

// ============================================================================
// Bench 1: 单连接直连
// ============================================================================
void benchDirect(int port, int total, int threads) {
    std::cout << "\n═══ Bench 1: Direct (1 conn, " << threads << " threads) ═══" << std::endl;

    RpcAsyncClient client("127.0.0.1", port);
    client.connect();

    Stats stats; std::mutex mtx;
    std::atomic<int> counter{0};
    std::vector<std::thread> workers;

    auto t0 = std::chrono::steady_clock::now();
    for (int w = 0; w < threads; ++w) {
        workers.emplace_back([&]() {
            while (true) {
                int i = counter.fetch_add(1);
                if (i >= total) break;
                double us = timedCall(client, "d" + std::to_string(i), 3000);
                std::lock_guard<std::mutex> lk(mtx);
                if (us < 0) stats.errors++; else stats.us.push_back(us);
            }
        });
    }
    for (auto& t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();

    client.disconnect();
    printStats("Direct", stats, std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================================
// Bench 2: 连接池
// ============================================================================
void benchPool(int port, int total, int threads, int poolSize) {
    std::cout << "\n═══ Bench 2: Pool (" << poolSize << " conns, " << threads << " threads) ═══" << std::endl;

    ConnectionPool pool;
    if (!pool.createPool("127.0.0.1", port, poolSize)) {
        std::cout << "  Pool creation failed!" << std::endl;
        return;
    }

    Stats stats; std::mutex mtx;
    std::atomic<int> counter{0};
    std::vector<std::thread> workers;

    auto t0 = std::chrono::steady_clock::now();
    for (int w = 0; w < threads; ++w) {
        workers.emplace_back([&]() {
            auto cp = pool.acquire("127.0.0.1", port);
            if (!cp) return;
            RpcAsyncClient client(cp);
            client.connect();
            while (true) {
                int i = counter.fetch_add(1);
                if (i >= total) break;
                double us = timedCall(client, "p" + std::to_string(i), 3000);
                std::lock_guard<std::mutex> lk(mtx);
                if (us < 0) stats.errors++; else stats.us.push_back(us);
            }
        });
    }
    for (auto& t : workers) t.join();
    auto t1 = std::chrono::steady_clock::now();

    printStats("Pool", stats, std::chrono::duration<double>(t1 - t0).count());
}

// ============================================================================
// Bench 3: 渐进增压
// ============================================================================
void benchScaling(int port, int baseTotal) {
    std::cout << "\n═══ Bench 3: Concurrency Scaling ═══" << std::endl;
    std::cout << "  Thr |     QPS |  Avg(us) |  P99(us)" << std::endl;
    std::cout << "  ─── | ─────── | ──────── | ────────" << std::endl;

    // 限制最大并发，避免线程/连接数过多
    for (int c : {1, 2, 4, 8}) {
        int n = std::max(200, baseTotal / c);

        // 给服务端时间清理旧连接（服务端单线程处理 accept + 业务）
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        ConnectionPool pool;
        if (!pool.createPool("127.0.0.1", port, c)) {
            std::cout << "  " << std::setw(3) << c << " | pool creation FAILED, skipping" << std::endl;
            continue;
        }

        std::atomic<int> counter{0};
        std::vector<double> samples;
        std::mutex mtx;

        auto t0 = std::chrono::steady_clock::now();
        std::vector<std::thread> workers;
        for (int w = 0; w < c; ++w) {
            workers.emplace_back([&]() {
                auto cp = pool.acquire("127.0.0.1", port);
                if (!cp) return;
                RpcAsyncClient cli(cp);
                cli.connect();
                for (int i = 0; i < n; ++i) {
                    double us = timedCall(cli, "s", 3000);
                    if (us > 0) { std::lock_guard<std::mutex> lk(mtx); samples.push_back(us); }
                }
                cli.disconnect();
            });
        }
        for (auto& t : workers) t.join();
        auto t1 = std::chrono::steady_clock::now();

        double elapsed = std::chrono::duration<double>(t1 - t0).count();
        double qps = (n * c) / elapsed;
        std::sort(samples.begin(), samples.end());
        double avg = samples.empty() ? 0 : std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();
        double p99 = samples.empty() ? 0 : samples[std::min(static_cast<size_t>(0.99 * samples.size()), samples.size() - 1)];

        std::cout << "  " << std::setw(3) << c << " | "
                  << std::setw(7) << std::fixed << std::setprecision(0) << qps << " | "
                  << std::setw(8) << avg << " | "
                  << std::setw(8) << p99 << std::endl;

        // 等待 pool 内的 EventLoopThread 完全退出后再进入下一轮
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

// ============================================================================
int main(int argc, char* argv[]) {
    int port = 19990;
    int total = (argc > 1) ? std::stoi(argv[1]) : 10000;
    int threads = (argc > 2) ? std::stoi(argv[2]) : 4;

    std::cout << "╔══════════════════════════════════╗" << std::endl;
    std::cout << "║  RPC Benchmark  " << std::setw(4) << total << " req  x " << std::setw(2) << threads << " threads  ║" << std::endl;
    std::cout << "╚══════════════════════════════════╝\n" << std::endl;

    // 启动服务端
    std::atomic<bool> serverQuit{false};
    std::thread serverThread([&]() {
        EventLoop loop;
        struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        RpcServer server(&loop, addr);
        server.registerService(std::make_shared<BenchEchoService>());
        server.start();
        loop.runEvery(0.5, [&]() { if (serverQuit.load()) loop.quit(); });
        loop.loop();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    benchDirect(port, total, threads);
    benchPool(port, total, threads, 4);
    benchScaling(port, total);

    serverQuit = true;
    serverThread.join();
    std::cout << "\nDone.\n" << std::endl;
}
