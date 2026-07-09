// src/loadbalance/consistent_hash.h
#ifndef CONSISTENT_HASH_H
#define CONSISTENT_HASH_H

#include <string>
#include <map>
#include <vector>
#include <unordered_map>
#include <functional>

namespace rpc {

class ConsistentHash {
public:
    using HashFunc = std::function<uint32_t(const std::string&)>;

    explicit ConsistentHash(int virtualNodes = 150);
    explicit ConsistentHash(HashFunc hashFunc, int virtualNodes = 150);

    // 添加物理节点
    void addNode(const std::string& node);
    
    // 删除物理节点
    void removeNode(const std::string& node);
    
    // 根据 key 获取节点
    std::string getNode(const std::string& key);
    
    // 获取节点数量
    size_t size() const;

private:
    // 默认 hash 函数（MurmurHash 简化版）
    static uint32_t defaultHash(const std::string& key);
    
    // 生成虚拟节点的 key
    std::string virtualNodeKey(const std::string& node, int index);

    int virtualNodes_;  // 每个物理节点的虚拟节点数
    HashFunc hashFunc_;
    
    // 哈希环：hash值 -> 物理节点名
    std::map<uint32_t, std::string> ring_;
    
    // 物理节点 -> 虚拟节点hash值列表
    std::unordered_map<std::string, std::vector<uint32_t>> nodeHashes_;
};

} // namespace rpc

#endif