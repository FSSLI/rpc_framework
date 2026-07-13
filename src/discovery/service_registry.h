// src/discovery/service_registry.h
#ifndef SERVICE_REGISTRY_H
#define SERVICE_REGISTRY_H

#include <string>
#include <vector>
#include <functional>

namespace rpc {

// 重命名避免和 Protobuf 生成的 ServiceNode 冲突
struct RegistryNode {
    std::string serviceName;
    std::string nodeId;
    std::string host;
    uint16_t port;
    uint64_t registerTime;
    uint64_t lastHeartbeat;
};

class ServiceRegistry {
public:
    virtual ~ServiceRegistry() = default;

    virtual bool registerService(const RegistryNode& node) = 0;
    virtual bool deregisterService(const std::string& serviceName, 
                                    const std::string& nodeId) = 0;
    virtual std::vector<RegistryNode> discover(const std::string& serviceName) = 0;
    virtual bool heartbeat(const std::string& serviceName,
                           const std::string& nodeId) = 0;

    using ServiceChangeCallback = std::function<void(const std::string& serviceName,
                                                      const std::vector<RegistryNode>& nodes)>;
    virtual void setServiceChangeCallback(const ServiceChangeCallback& cb) = 0;
};

} // namespace rpc

#endif