// tests/test_memory_registry.cc
#include "discovery/memory_registry.h"
#include "discovery/service_registry.h"
#include <iostream>
#include <cassert>
#include <memory>
#include <functional>
#include <atomic>
#include <chrono>  // ← 新增
#include <netinet/in.h>

using namespace rpc;

int main() {
    auto registry = std::make_shared<MemoryRegistry>();
    
    // 使用 RegistryNode
    RegistryNode node1{"EchoService", "node_1", "127.0.0.1", 8888, 0, 0};
    RegistryNode node2{"EchoService", "node_2", "127.0.0.1", 8889, 0, 0};
    
    assert(registry->registerService(node1));
    assert(registry->registerService(node2));
    
    auto nodes = registry->discover("EchoService");
    assert(nodes.size() == 2);
    std::cout << "Discovered " << nodes.size() << " nodes" << std::endl;
    
    assert(registry->deregisterService("EchoService", "node_1"));
    nodes = registry->discover("EchoService");
    assert(nodes.size() == 1);
    
    std::cout << "✅ MemoryRegistry test passed!" << std::endl;
    return 0;
}