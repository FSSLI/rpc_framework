// src/loadbalance/consistent_hash.cc
#include "loadbalance/consistent_hash.h"
#include <sstream>

namespace rpc {

ConsistentHash::ConsistentHash(int virtualNodes)
    : ConsistentHash(defaultHash, virtualNodes) {
}

ConsistentHash::ConsistentHash(HashFunc hashFunc, int virtualNodes)
    : virtualNodes_(virtualNodes),
      hashFunc_(hashFunc) {
}

void ConsistentHash::addNode(const std::string& node) {
    std::lock_guard<std::mutex> lock(mutex_);  // ← 写锁
    
    // 如果节点已存在，先删除
    auto it = nodeHashes_.find(node);
    if (it != nodeHashes_.end()) {
        for (uint32_t hash : it->second) {
            ring_.erase(hash);
        }
        nodeHashes_.erase(it);
    }
    
    std::vector<uint32_t> hashes;
    hashes.reserve(virtualNodes_);
    
    for (int i = 0; i < virtualNodes_; ++i) {
        std::string vKey = virtualNodeKey(node, i);
        uint32_t hash = hashFunc_(vKey);
        
        ring_[hash] = node;
        hashes.push_back(hash);
    }
    
    nodeHashes_[node] = std::move(hashes);
}

void ConsistentHash::removeNode(const std::string& node) {
    std::lock_guard<std::mutex> lock(mutex_);  // ← 写锁
    
    auto it = nodeHashes_.find(node);
    if (it == nodeHashes_.end()) {
        return;
    }
    
    for (uint32_t hash : it->second) {
        ring_.erase(hash);
    }
    
    nodeHashes_.erase(it);
}

std::string ConsistentHash::getNode(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);  // ← 读锁（用 mutex 代替 shared_mutex，C++14 兼容）
    
    if (ring_.empty()) {
        return "";
    }
    
    uint32_t hash = hashFunc_(key);
    
    // 顺时针找第一个 >= hash 的节点
    auto it = ring_.lower_bound(hash);
    
    // 如果到了环尾，回到开头
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    
    return it->second;
}

size_t ConsistentHash::size() const {
    std::lock_guard<std::mutex> lock(mutex_);  // ← 读锁
    return nodeHashes_.size();
}

uint32_t ConsistentHash::defaultHash(const std::string& key) {
    // MurmurHash2 简化版（保持不变）
    const uint32_t seed = 0x9747b28c;
    const uint32_t m = 0x5bd1e995;
    const int r = 24;
    
    uint32_t len = key.length();
    const char* data = key.data();
    uint32_t h = seed ^ len;
    
    while (len >= 4) {
        uint32_t k = *(uint32_t*)data;
        
        k *= m;
        k ^= k >> r;
        k *= m;
        
        h *= m;
        h ^= k;
        
        data += 4;
        len -= 4;
    }
    
    switch (len) {
        case 3: h ^= data[2] << 16;
        case 2: h ^= data[1] << 8;
        case 1: h ^= data[0];
                h *= m;
    }
    
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    
    return h;
}

std::string ConsistentHash::virtualNodeKey(const std::string& node, int index) {
    std::ostringstream oss;
    oss << node << "#" << index;
    return oss.str();
}

} // namespace rpc