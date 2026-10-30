// tests/bench_pipeline.cc — 流水线（pipelined）压测
//
// 与 test_benchmark.cc 的区别：
//   test_benchmark 是"闭环同步"：每线程发一个请求、future.get() 等回来再发下一个，
//   所以在途请求数 == 线程数，吞吐 = 在途 / RTT，测到的是延迟，不是上限。
//
//   本程序用回调 API（真异步路径），每条连接维持 window 个请求在途，
//   在途数 = conns * window，可以远大于线程数，用来压出框架的真实吞吐上限。
//
// 用法： ./bench_pipeline [连接数] [每连接在途数] [总请求数] [服务端从Reactor数]
// 默认： 1 64 200000 4
//
// 注意：回调是在客户端 EventLoop 线程里、且持有 pendingMutex_ 时被调用的，
//       所以回调里不能再调 asyncCall（会自锁）。这里由独立提交线程在窗口空位时补发。

#include "client/rpc_async_client.h"
#include "network/event_loop.h"
#include "pool/connection_pool.h"
#include "protocol/rpc_service.pb.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <set>
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

// ============================================================================
struct ConnStats {
    std::mutex m;
    std::condition_variable cv;
    std::vector<double> latUs;   // <0 表示失败/超时
    int inflight = 0;
    long errors = 0;
    long done = 0;
};

static double pct(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0;
    size_t i = static_cast<size_t>(p * (sorted.size() - 1));
    return sorted[i];
}

// ============================================================================
int main(int argc, char* argv[]) {
    int port          = 19991;
    int conns         = (argc > 1) ? std::stoi(argv[1]) : 1;
    int window        = (argc > 2) ? std::stoi(argv[2]) : 64;
    long total        = (argc > 3) ? std::stol(argv[3]) : 200000;
    int serverThreads = (argc > 4) ? std::stoi(argv[4]) : 4;
    const int kTimeoutMs = 5000;

    if (conns <= 0 || window <= 0 || total <= 0) {
        std::cerr << "invalid args\n";
        return 2;
    }

    std::cout << "╔══════════════════════════════════════════════════════╗\n"
              << "║  Pipeline Benchmark                                  ║\n"
              << "║  conns=" << std::setw(3) << conns
              << "  window=" << std::setw(4) << window
              << "  inflight=" << std::setw(5) << (conns * window)
              << "  req=" << std::setw(7) << total << " ║\n"
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

    // ---------------- 客户端：每连接一个 RpcAsyncClient ----------------
    ConnectionPool pool;
    if (!pool.createPool("127.0.0.1", static_cast<uint16_t>(port), conns)) {
        std::cerr << "pool creation failed\n";
        quit.store(true);
        serverThread.join();
        return 1;
    }

    std::vector<std::shared_ptr<TcpClient>> tcpClients;
    std::vector<std::unique_ptr<RpcAsyncClient>> clients(conns);
    for (int i = 0; i < conns; ++i) {
        auto cp = pool.acquire("127.0.0.1", static_cast<uint16_t>(port));
        if (!cp) {
            std::cerr << "acquire failed at " << i << "\n";
            quit.store(true);
            serverThread.join();
            return 1;
        }
        tcpClients.push_back(cp);
        clients[i] = std::make_unique<RpcAsyncClient>(cp);
        clients[i]->connect();
    }

    // 防御：连接数 < 线程数时 acquire 会轮询返回同一个 TcpClient，
    // 多个 RpcAsyncClient 共享一条连接会互相覆盖回调（响应投递错位）。
    {
        std::set<const TcpClient*> uniq;
        for (auto& c : tcpClients) uniq.insert(c.get());
        if (static_cast<int>(uniq.size()) != conns) {
            std::cerr << "ERROR: " << conns << " 个 client 只拿到 " << uniq.size()
                      << " 个不同的 TcpClient（连接池大小不足以支撑该并发）\n";
            quit.store(true);
            serverThread.join();
            return 1;
        }
    }

    std::vector<std::unique_ptr<ConnStats>> stats(conns);
    for (auto& s : stats) s = std::make_unique<ConnStats>();

    // ---------------- 起跑 ----------------
    std::atomic<long> sent{0};
    const auto t0 = Clock::now();

    std::vector<std::thread> subs;
    subs.reserve(conns);
    for (int i = 0; i < conns; ++i) {
        subs.emplace_back([&, i]() {
            ConnStats& st = *stats[i];
            RpcAsyncClient& cli = *clients[i];
            // 总请求均分到各连接
            long remaining = total / conns + (i < static_cast<int>(total % conns) ? 1 : 0);

            while (remaining > 0) {
                // 等窗口有空位（在锁内只做计数，发送在锁外）
                {
                    std::unique_lock<std::mutex> lk(st.m);
                    st.cv.wait(lk, [&]() { return st.inflight < window; });
                    ++st.inflight;
                }

                EchoRequest echoReq;
                echoReq.set_message("pipeline");
                RpcRequest req;
                req.set_payload(echoReq.SerializeAsString());

                const auto start = Clock::now();
                cli.asyncCall(
                    "EchoService", "Echo", req,
                    [&st, start](const RpcResponse& resp) {
                        double us = std::chrono::duration<double, std::micro>(
                                        Clock::now() - start).count();
                        {
                            std::lock_guard<std::mutex> lk(st.m);
                            st.latUs.push_back(resp.success() ? us : -1.0);
                            if (!resp.success()) ++st.errors;
                            --st.inflight;
                            ++st.done;
                        }
                        st.cv.notify_one();
                    },
                    kTimeoutMs);

                --remaining;
                ++sent;
            }
        });
    }
    for (auto& t : subs) t.join();   // 全部请求已发出

    // ---------------- 等在途请求全部返回（带看门狗） ----------------
    bool timedOut = false;
    const auto deadline = Clock::now() + std::chrono::seconds(120);
    while (true) {
        int pending = 0;
        for (auto& s : stats) {
            std::lock_guard<std::mutex> lk(s->m);
            pending += s->inflight;
        }
        if (pending == 0) break;
        if (Clock::now() > deadline) { timedOut = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const auto t1 = Clock::now();
    const double elapsed = std::chrono::duration<double>(t1 - t0).count();

    // ---------------- 汇总 ----------------
    std::vector<double> ok;
    long errors = 0, done = 0;
    ok.reserve(static_cast<size_t>(total));
    for (auto& s : stats) {
        std::lock_guard<std::mutex> lk(s->m);
        errors += s->errors;
        done += s->done;
        for (double us : s->latUs) {
            if (us > 0) ok.push_back(us);
        }
    }
    std::sort(ok.begin(), ok.end());

    double sum = 0;
    for (double v : ok) sum += v;

    std::cout << "\n  conns=" << conns << "  window=" << window
              << "  serverThreads=" << serverThreads << "\n";
    std::cout << "  Sent: " << sent.load() << "  Done: " << done
              << "  Errors: " << errors
              << (timedOut ? "  [TIMEOUT: 仍有请求未返回]" : "") << "\n";
    std::cout << "  Elapsed: " << std::fixed << std::setprecision(3) << elapsed << "s"
              << "  QPS: " << std::setprecision(0) << (done / elapsed) << "/s\n";
    std::cout << "  Latency(us) → avg: " << std::setprecision(1)
              << (ok.empty() ? 0.0 : sum / static_cast<double>(ok.size()))
              << "  p50: " << pct(ok, 0.50)
              << "  p99: " << pct(ok, 0.99)
              << "  p999: " << pct(ok, 0.999) << "\n";

    clients.clear();
    tcpClients.clear();
    quit.store(true);
    serverThread.join();
    return (errors == 0 && !timedOut) ? 0 : 1;
}
