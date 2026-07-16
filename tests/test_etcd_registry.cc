// tests/test_etcd_registry.cc
// etcd 服务发现测试 — 需先启动 etcd: docker run -d --name etcd-test -p 2379:2379 bitnami/etcd:3.5
#ifdef HAS_ETCD

#include "discovery/etcd_registry.h"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <vector>

using namespace rpc;

// ============================================================================
bool testRegisterDiscover() {
    std::cout << "=== Test 1: Register & Discover ===" << std::endl;

    EtcdRegistry reg("http://127.0.0.1:2379", 10, "/rpc_test/services");

    RegistryNode n1{"TestService", "node1", "127.0.0.1", 9001, 0, 0};
    RegistryNode n2{"TestService", "node2", "127.0.0.1", 9002, 0, 0};

    assert(reg.registerService(n1));
    assert(reg.registerService(n2));

    auto nodes = reg.discover("TestService");
    assert(nodes.size() == 2);
    std::cout << "  Discovered " << nodes.size() << " nodes" << std::endl;

    assert(reg.deregisterService("TestService", "node1"));
    nodes = reg.discover("TestService");
    assert(nodes.size() == 1);

    std::cout << "  ✅ Register/Discover test passed" << std::endl;
    return true;
}

// ============================================================================
bool testWatchCallback() {
    std::cout << "=== Test 2: Watch Callback ===" << std::endl;

    EtcdRegistry reg("http://127.0.0.1:2379", 10, "/rpc_test/services_watch");

    std::mutex mtx;
    std::vector<std::string> notifications;
    std::atomic<bool> received{false};

    reg.setServiceChangeCallback([&](const std::string& svc, const std::vector<RegistryNode>& nodes) {
        std::lock_guard<std::mutex> lk(mtx);
        notifications.push_back(svc + ":" + std::to_string(nodes.size()));
        received = true;
    });

    RegistryNode n{"WatchService", "w1", "127.0.0.1", 9100, 0, 0};
    assert(reg.registerService(n));

    // 等待 Watch 回调（最多 5 秒）
    for (int i = 0; i < 50 && !received.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    assert(received.load());
    std::cout << "  Watch notification: " << notifications[0] << std::endl;
    std::cout << "  ✅ Watch callback test passed" << std::endl;

    reg.deregisterService("WatchService", "w1");
    return true;
}

int main() {
    std::cout << "=== EtcdRegistry Test (requires etcd on localhost:2379) ===" << std::endl;
    bool ok = testRegisterDiscover() && testWatchCallback();
    std::cout << (ok ? "\nAll etcd tests passed!\n" : "\nFAILED\n");
    return ok ? 0 : 1;
}

#else
#include <iostream>
int main() { std::cout << "etcd test skipped (build with -DWITH_ETCD=ON)\n"; return 0; }
#endif
