// src/discovery/memory_registry.h
#ifndef MEMORY_REGISTRY_H
#define MEMORY_REGISTRY_H

#include "discovery/service_registry.h"
#include <mutex>
#include <unordered_map>
#include <chrono>

namespace rpc {

class MemoryRegistry : public ServiceRegistry {
public:
    MemoryRegistry();
    ~MemoryRegistry() override = default;

    bool registerService(const RegistryNode& node) override;
    bool deregisterService(const std::string& serviceName,
                           const std::string& nodeId) override;
    std::vector<RegistryNode> discover(const std::string& serviceName) override;
    bool heartbeat(const std::string& serviceName,
                   const std::string& nodeId) override;
    void setServiceChangeCallback(const ServiceChangeCallback& cb) override;

    void cleanupExpiredNodes(int timeoutSeconds);

private:
    using NodeMap = std::unordered_map<std::string, RegistryNode>;
    using ServiceMap = std::unordered_map<std::string, NodeMap>;

    ServiceMap services_;
    mutable std::mutex mutex_;
    ServiceChangeCallback changeCallback_;
};

} // namespace rpc

#endif