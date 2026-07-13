// src/discovery/memory_registry.cc
#include "discovery/memory_registry.h"
#include <iostream>

namespace rpc {

MemoryRegistry::MemoryRegistry() = default;

bool MemoryRegistry::registerService(const RegistryNode& node) {
    bool isNew = false;
    std::vector<RegistryNode> nodes;
    bool shouldNotify = false;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto& nodeMap = services_[node.serviceName];
        isNew = (nodeMap.find(node.nodeId) == nodeMap.end());

        // 自动设置注册时间和心跳时间，防止 cleanupExpiredNodes 误清理
        RegistryNode storedNode = node;
        auto nowTs = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        if (storedNode.registerTime == 0) {
            storedNode.registerTime = nowTs;
        }
        storedNode.lastHeartbeat = nowTs;

        nodeMap[node.nodeId] = storedNode;

        std::cout << "MemoryRegistry: register " << node.serviceName 
                  << " node " << node.nodeId 
                  << " at " << node.host << ":" << node.port << std::endl;

        // 锁内只拷贝数据，不回调
        if (changeCallback_) {
            shouldNotify = true;
            for (const auto& pair : nodeMap) {
                nodes.push_back(pair.second);
            }
        }
    }

    // 锁外回调，避免死锁
    if (shouldNotify) {
        changeCallback_(node.serviceName, nodes);
    }

    return true;
}

bool MemoryRegistry::deregisterService(const std::string& serviceName,
                                         const std::string& nodeId) {
    std::vector<RegistryNode> nodes;
    bool shouldNotify = false;
    bool removed = false;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = services_.find(serviceName);
        if (it == services_.end()) return false;

        auto& nodeMap = it->second;
        auto nodeIt = nodeMap.find(nodeId);
        if (nodeIt == nodeMap.end()) return false;

        nodeMap.erase(nodeIt);
        removed = true;
        std::cout << "MemoryRegistry: deregister " << serviceName 
                  << " node " << nodeId << std::endl;

        if (changeCallback_) {
            shouldNotify = true;
            for (const auto& pair : nodeMap) {
                nodes.push_back(pair.second);
            }
        }

        if (nodeMap.empty()) {
            services_.erase(it);
        }
    }

    if (shouldNotify) {
        changeCallback_(serviceName, nodes);
    }

    return removed;
}

std::vector<RegistryNode> MemoryRegistry::discover(const std::string& serviceName) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<RegistryNode> result;
    auto it = services_.find(serviceName);
    if (it != services_.end()) {
        for (const auto& pair : it->second) {
            result.push_back(pair.second);
        }
    }
    return result;
}

bool MemoryRegistry::heartbeat(const std::string& serviceName,
                                 const std::string& nodeId) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = services_.find(serviceName);
    if (it == services_.end()) return false;

    auto& nodeMap = it->second;
    auto nodeIt = nodeMap.find(nodeId);
    if (nodeIt == nodeMap.end()) return false;

    // Issue #3 fix: 改用 monotonic steady_clock，防止系统时间回拨误杀节点
    nodeIt->second.lastHeartbeat =
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();

    return true;
}

void MemoryRegistry::setServiceChangeCallback(const ServiceChangeCallback& cb) {
    changeCallback_ = cb;
}

void MemoryRegistry::cleanupExpiredNodes(int timeoutSeconds) {
    std::vector<std::pair<std::string, std::vector<RegistryNode>>> notifications;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto now = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();

        for (auto serviceIt = services_.begin(); serviceIt != services_.end(); ) {
            auto& nodeMap = serviceIt->second;

            for (auto nodeIt = nodeMap.begin(); nodeIt != nodeMap.end(); ) {
                if (now - nodeIt->second.lastHeartbeat > timeoutSeconds) {
                    std::cout << "MemoryRegistry: cleanup expired node " 
                              << nodeIt->first << " of " << serviceIt->first << std::endl;
                    nodeIt = nodeMap.erase(nodeIt);
                } else {
                    ++nodeIt;
                }
            }

            if (nodeMap.empty()) {
                // Issue #8 fix: 服务所有节点下线，发送空列表通知
                if (changeCallback_) {
                    notifications.emplace_back(serviceIt->first, std::vector<RegistryNode>{});
                }
                serviceIt = services_.erase(serviceIt);
            } else {
                if (changeCallback_) {
                    std::vector<RegistryNode> nodes;
                    for (const auto& pair : nodeMap) {
                        nodes.push_back(pair.second);
                    }
                    notifications.emplace_back(serviceIt->first, std::move(nodes));
                }
                ++serviceIt;
            }
        }
    }

    // 锁外批量回调
    for (auto& notification : notifications) {
        changeCallback_(notification.first, notification.second);
    }
}

} // namespace rpc