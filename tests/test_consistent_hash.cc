// tests/test_consistent_hash.cc
#include "loadbalance/consistent_hash.h"
#include <iostream>
#include <cassert>

using namespace rpc;

int main() {
    ConsistentHash ch(150);  // 每个节点150个虚拟节点
    
    // 添加节点
    ch.addNode("192.168.1.1:8080");
    ch.addNode("192.168.1.2:8080");
    ch.addNode("192.168.1.3:8080");
    
    std::cout << "nodes: " << ch.size() << std::endl;
    
    // 测试分布
    std::map<std::string, int> distribution;
    for (int i = 0; i < 10000; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string node = ch.getNode(key);
        distribution[node]++;
    }
    
    std::cout << "distribution:" << std::endl;
    for (const auto& pair : distribution) {
        std::cout << "  " << pair.first << ": " << pair.second << std::endl;
    }
    
    // 测试节点删除
    std::string testKey = "test_key_123";
    std::string node1 = ch.getNode(testKey);
    std::cout << "before remove, " << testKey << " -> " << node1 << std::endl;
    
    ch.removeNode("192.168.1.2:8080");
    
    std::string node2 = ch.getNode(testKey);
    std::cout << "after remove, " << testKey << " -> " << node2 << std::endl;
    
    // 验证只有被删除节点的 key 会迁移
    int changed = 0;
    for (int i = 0; i < 10000; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string node = ch.getNode(key);
        if (node == "192.168.1.2:8080") {
            changed++;
        }
    }
    std::cout << "keys need to migrate: " << changed << std::endl;
    
    return 0;
}