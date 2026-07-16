// src/discovery/etcd_registry.h
// etcd 服务注册/发现 + Watch 实时感知
// 依赖: etcd-cpp-apiv3 (https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3)
// 编译: cmake .. -DWITH_ETCD=ON

#ifndef ETCD_REGISTRY_H
#define ETCD_REGISTRY_H

#ifdef HAS_ETCD

#include "discovery/service_registry.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <thread>
#include <atomic>

namespace etcd { class Client; class Watcher; }

namespace rpc {

class EtcdRegistry : public ServiceRegistry {
public:
    // etcdEndpoints: "http://127.0.0.1:2379"
    // leaseTTLSeconds: 服务注册的租约 TTL（超时自动清除）
    // keyPrefix: etcd key 前缀，如 "/rpc/services"
    EtcdRegistry(const std::string& etcdEndpoints,
                 int64_t leaseTTLSeconds = 30,
                 const std::string& keyPrefix = "/rpc/services");
    ~EtcdRegistry() override;

    bool registerService(const RegistryNode& node) override;
    bool deregisterService(const std::string& serviceName,
                           const std::string& nodeId) override;
    std::vector<RegistryNode> discover(const std::string& serviceName) override;
    bool heartbeat(const std::string& serviceName,
                   const std::string& nodeId) override;
    void setServiceChangeCallback(const ServiceChangeCallback& cb) override;

private:
    std::string nodeKey(const std::string& serviceName, const std::string& nodeId) const;
    std::string servicePrefix(const std::string& serviceName) const;
    RegistryNode parseNode(const std::string& key, const std::string& value) const;
    void startWatchLoop(const std::string& serviceName);
    void notifyChange(const std::string& serviceName);

    std::string endpoints_;
    int64_t leaseTTL_;
    std::string prefix_;
    int64_t leaseId_ = 0;

    std::unique_ptr<etcd::Client> client_;
    mutable std::mutex mutex_;

    // 每个服务的 watching 标志（避免重复启动 Watch 线程）
    std::unordered_map<std::string, bool> watching_;
    std::unique_ptr<std::thread> keepAliveThread_;
    std::atomic<bool> running_{true};

    ServiceChangeCallback changeCallback_;
};

} // namespace rpc

#else
// 无 etcd 依赖时提供空实现声明
#endif // HAS_ETCD
#endif // ETCD_REGISTRY_H
