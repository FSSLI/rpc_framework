// src/discovery/etcd_registry.cc
#ifdef HAS_ETCD

#include "discovery/etcd_registry.h"
#include <etcd/Client.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/Watcher.hpp>
#include <iostream>
#include <sstream>
#include <nlohmann/json.hpp>  // 需安装 nlohmann-json

using json = nlohmann::json;

namespace rpc {

// JSON 序列化/反序列化 RegistryNode
static std::string nodeToJson(const RegistryNode& node) {
    json j;
    j["serviceName"] = node.serviceName;
    j["nodeId"] = node.nodeId;
    j["host"] = node.host;
    j["port"] = node.port;
    j["registerTime"] = node.registerTime;
    j["lastHeartbeat"] = node.lastHeartbeat;
    return j.dump();
}

static RegistryNode jsonToNode(const std::string& key, const std::string& value) {
    RegistryNode node;
    try {
        json j = json::parse(value);
        node.serviceName = j.value("serviceName", "");
        node.nodeId = j.value("nodeId", "");
        node.host = j.value("host", "");
        node.port = j.value("port", 0);
        node.registerTime = j.value("registerTime", uint64_t(0));
        node.lastHeartbeat = j.value("lastHeartbeat", uint64_t(0));
    } catch (...) {
        std::cerr << "EtcdRegistry: failed to parse JSON for key " << key << std::endl;
    }
    return node;
}

// ============================================================================

EtcdRegistry::EtcdRegistry(const std::string& etcdEndpoints,
                           int64_t leaseTTLSeconds,
                           const std::string& keyPrefix)
    : endpoints_(etcdEndpoints),
      leaseTTL_(leaseTTLSeconds),
      prefix_(keyPrefix) {
    client_ = std::make_unique<etcd::Client>(etcdEndpoints);
    // 创建租约并启动 keep-alive
    auto lease = client_->leasegrant(leaseTTL_).get();
    if (lease.is_ok()) {
        leaseId_ = lease.value().lease();
        // 后台 keep-alive 线程
        keepAliveThread_ = std::make_unique<std::thread>([this]() {
            etcd::KeepAlive ka(*client_, 3, leaseId_);
            while (running_.load()) {
                ka.Check();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        });
    }
}

EtcdRegistry::~EtcdRegistry() {
    running_ = false;
    if (keepAliveThread_ && keepAliveThread_->joinable()) {
        keepAliveThread_->join();
    }
}

std::string EtcdRegistry::nodeKey(const std::string& serviceName,
                                   const std::string& nodeId) const {
    return prefix_ + "/" + serviceName + "/" + nodeId;
}

std::string EtcdRegistry::servicePrefix(const std::string& serviceName) const {
    return prefix_ + "/" + serviceName + "/";
}

bool EtcdRegistry::registerService(const RegistryNode& node) {
    std::string key = nodeKey(node.serviceName, node.nodeId);
    std::string value = nodeToJson(node);
    auto resp = client_->put(key, value, leaseId_).get();
    if (!resp.is_ok()) return false;


    std::cout << "EtcdRegistry: registered " << key << " (lease=" << leaseId_ << ")" << std::endl;

    // 通知变更
    notifyChange(node.serviceName);

    // 首次注册该服务时启动 Watch
    startWatchLoop(node.serviceName);
    return true;
}

bool EtcdRegistry::deregisterService(const std::string& serviceName,
                                      const std::string& nodeId) {
    std::string key = nodeKey(serviceName, nodeId);
    auto resp = client_->rm(key).get();
    bool ok = resp.is_ok();
    if (ok) {
        std::cout << "EtcdRegistry: deregistered " << key << std::endl;
        notifyChange(serviceName);
    }
    return ok;
}

std::vector<RegistryNode> EtcdRegistry::discover(const std::string& serviceName) {
    std::string prefix = servicePrefix(serviceName);
    auto resp = client_->ls(prefix).get();
    std::vector<RegistryNode> nodes;
    if (resp.is_ok()) {
        for (auto& kv : resp.values()) {
            // kv.key(): "/rpc/services/EchoService/node1"
            // kv.as_string(): JSON of RegistryNode
            std::string key = kv.key();
            nodes.push_back(jsonToNode(key, kv.as_string()));
        }
    }
    return nodes;
}

bool EtcdRegistry::heartbeat(const std::string& serviceName,
                              const std::string& nodeId) {
    // etcd lease keep-alive 已自动续约，无需手动心跳
    (void)serviceName;
    (void)nodeId;
    return true;
}

void EtcdRegistry::setServiceChangeCallback(const ServiceChangeCallback& cb) {
    changeCallback_ = cb;
}

void EtcdRegistry::startWatchLoop(const std::string& serviceName) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (watching_[serviceName]) return;  // 已在监听
    watching_[serviceName] = true;

    std::string prefix = servicePrefix(serviceName);
    std::thread([this, serviceName, prefix]() {
        etcd::Watcher watcher(*client_, prefix, 0);
        while (running_.load()) {
            bool ok = watcher.Wait();
            if (ok) {
                notifyChange(serviceName);
            }
        }
    }).detach();
}

void EtcdRegistry::notifyChange(const std::string& serviceName) {
    if (!changeCallback_) return;
    auto nodes = discover(serviceName);
    changeCallback_(serviceName, nodes);
}

} // namespace rpc

#endif // HAS_ETCD
