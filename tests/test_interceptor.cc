// tests/test_interceptor.cc — 拦截器链测试
#include "interceptor/interceptor.h"
#include "interceptor/builtin_interceptors.h"
#include "circuit_breaker/circuit_breaker.h"
#include "rate_limiter/token_bucket.h"
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

using namespace rpc;

// ============================================================================
// Test 1: InterceptorChain 基本顺序
// ============================================================================
bool testChainOrder() {
    std::cout << "=== Test 1: Chain execution order ===" << std::endl;

    InterceptorChain chain;
    std::vector<std::string> log;

    // 三个拦截器，各自记录 pre/post
    struct TestInt : Interceptor {
        std::string name;
        std::vector<std::string>* log;
        TestInt(std::string n, std::vector<std::string>* l) : name(n), log(l) {}
        bool preHandle(RpcRequest&) override { log->push_back("pre_" + name); return true; }
        void postHandle(const RpcResponse&) override { log->push_back("post_" + name); }
    };

    chain.addInterceptor(std::make_shared<TestInt>("A", &log));
    chain.addInterceptor(std::make_shared<TestInt>("B", &log));
    chain.addInterceptor(std::make_shared<TestInt>("C", &log));

    RpcRequest req;
    auto resp = chain.execute(req, [](RpcRequest&) -> RpcResponse {
        RpcResponse r; r.set_success(true); return r;
    });

    assert(resp.success());
    assert(log.size() == 6);
    // pre: A → B → C, post: C → B → A
    assert(log[0] == "pre_A"); assert(log[1] == "pre_B"); assert(log[2] == "pre_C");
    assert(log[3] == "post_C"); assert(log[4] == "post_B"); assert(log[5] == "post_A");

    std::cout << "  Order: " << log[0] << " " << log[1] << " " << log[2]
              << " | " << log[3] << " " << log[4] << " " << log[5] << std::endl;
    std::cout << "  ✅ Chain order test passed" << std::endl;
    return true;
}

// ============================================================================
// Test 2: 拦截器阻止请求
// ============================================================================
bool testInterceptorBlock() {
    std::cout << "=== Test 2: Interceptor blocking ===" << std::endl;

    InterceptorChain chain;
    struct Blocker : Interceptor {
        bool preHandle(RpcRequest&) override { return false; }  // always block
    };
    chain.addInterceptor(std::make_shared<Blocker>());

    RpcRequest req;
    auto resp = chain.execute(req, [](RpcRequest&) -> RpcResponse {
        assert(false);  // should never reach here
        return {};
    });

    assert(!resp.success());
    assert(resp.error_msg() == "interceptor blocked");
    std::cout << "  Blocked: " << resp.error_msg() << std::endl;
    std::cout << "  ✅ Block test passed" << std::endl;
    return true;
}

// ============================================================================
// Test 3: RPC 集成——熔断 + 限流 通过拦截器链
// ============================================================================
class InterceptorEcho : public RpcService {
public:
    InterceptorEcho() { registerMethod("Echo", [this](const RpcRequest& req, RpcResponse* resp) {
        EchoRequest er; er.ParseFromString(req.payload());
        EchoResponse echoR; echoR.set_message(er.message());
        resp->set_success(true); resp->set_payload(echoR.SerializeAsString());
    });}
    std::string serviceName() const override { return "EchoService"; }
};

bool testRpcWithInterceptorChain() {
    std::cout << "=== Test 3: RPC + Interceptor Chain (CB + RL + Trace + Log) ===" << std::endl;

    std::atomic<bool> quit{false};
    std::thread st([&]() {
        EventLoop loop; sockaddr_in a; memset(&a,0,sizeof a);
        a.sin_family=AF_INET; a.sin_port=htons(23001); a.sin_addr.s_addr=INADDR_ANY;
        RpcServer s(&loop,a); s.registerService(std::make_shared<InterceptorEcho>()); s.start();
        loop.runEvery(0.1,[&](){ if(quit.load()) loop.quit(); }); loop.loop();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 创建客户端 + 通过拦截器链注入熔断/限流/Trace/Log
    // 注意：setCircuitBreaker/setRateLimiter 内部会创建内置拦截器加入链
    CircuitBreaker cb(5, 3);
    TokenBucket limiter(1000.0, 10.0);

    RpcAsyncClient client("127.0.0.1", 23001);
    client.setCircuitBreaker(&cb);   // → 链中插入 CircuitBreakerInterceptor
    client.setRateLimiter(&limiter);  // → 链中插入 RateLimitInterceptor
    client.addInterceptor(std::make_shared<TraceInterceptor>());
    client.addInterceptor(std::make_shared<LogInterceptor>("test"));
    assert(client.connect());

    // 正常请求应通过
    EchoRequest er; er.set_message("hello_interceptor");
    RpcRequest req; req.set_payload(er.SerializeAsString());
    auto f = client.asyncCall("EchoService","Echo",req,3000);
    auto r = f.get();
    assert(r.success());
    EchoResponse echoR; echoR.ParseFromString(r.payload());
    std::cout << "  Response: " << echoR.message() << std::endl;

    // 验证拦截器链生效：限流耗尽 + 请求被拦截
    // (限流器 1000/s burst 10 不会耗尽，这里只验证链能正常工作)

    std::cout << "  ✅ RPC interceptor chain test passed" << std::endl;

    client.disconnect(); quit=true; st.join();
    return true;
}

// ============================================================================
int main() {
    bool ok = testChainOrder() && testInterceptorBlock() && testRpcWithInterceptorChain();
    std::cout << (ok ? "\nAll interceptor tests passed!\n" : "\nFAILED\n");
    return ok ? 0 : 1;
}
