// src/pool/connection_pool.cc
#include "pool/connection_pool.h"
#include "network/tcp_client.h"
#include "network/tcp_connection.h"
#include "network/event_loop_thread.h"
#include "network/event_loop.h"
#include <arpa/inet.h>
#include <cstring>
#include <future>
#include <iostream>

namespace rpc {

ConnectionPool::~ConnectionPool() {
    // 先停止所有 TcpClient，再停止 EventLoopThread
    // unique_ptr 自动处理析构顺序
    for (auto& pair : pools_) {
        auto& ep = pair.second;
        for (auto& client : ep->clients) {
            if (client) {
                client->stop();
            }
        }
        // EventLoopThread 由 unique_ptr 在 EndpointPool 析构时自动清理
    }
}

std::string ConnectionPool::makeKey(const std::string& host, uint16_t port) {
    return host + ":" + std::to_string(port);
}

bool ConnectionPool::createPool(const std::string& host, uint16_t port, int poolSize) {
    if (poolSize <= 0) {
        std::cerr << "ConnectionPool::createPool: invalid poolSize=" << poolSize << std::endl;
        return false;
    }

    std::string key = makeKey(host, port);

    // 构建 serverAddr
    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &serverAddr.sin_addr) <= 0) {
        std::cerr << "ConnectionPool::createPool: invalid IP " << host << std::endl;
        return false;
    }

    auto ep = std::make_unique<EndpointPool>();
    ep->clients.reserve(poolSize);
    ep->loops.reserve(poolSize);

    for (int i = 0; i < poolSize; ++i) {
        // 1. 创建独立的 EventLoopThread
        auto loopThread = std::make_unique<EventLoopThread>();
        EventLoop* loop = loopThread->startLoop();

        // 2. 创建 TcpClient
        auto client = std::make_shared<TcpClient>(loop, serverAddr);
        client->setRetryOnDisconnect(true);  // 断开自动重连

        // 3. 等待连接建立
        std::promise<bool> connectPromise;
        auto connectFuture = connectPromise.get_future();
        std::atomic<bool> promiseSet{false};

        client->setConnectionCallback(
            [&connectPromise, &promiseSet](const TcpConnectionPtr& conn) {
                if (conn->connected()) {
                    bool expected = false;
                    if (promiseSet.compare_exchange_strong(expected, true)) {
                        connectPromise.set_value(true);
                    }
                }
            });

        client->connect();

        auto status = connectFuture.wait_for(std::chrono::milliseconds(3000));
        if (status == std::future_status::timeout) {
            bool expected = false;
            if (promiseSet.compare_exchange_strong(expected, true)) {
                connectPromise.set_value(false);
            }
            std::cerr << "ConnectionPool: connection " << i
                      << " to " << key << " timed out, aborting pool creation" << std::endl;
            // 清理已创建的连接
            client->stop();
            loopThread->stop();
            return false;
        }

        if (!connectFuture.get()) {
            std::cerr << "ConnectionPool: connection " << i
                      << " to " << key << " failed, aborting pool creation" << std::endl;
            client->stop();
            loopThread->stop();
            return false;
        }

        ep->clients.push_back(std::move(client));
        ep->loops.push_back(std::move(loopThread));
    }

    std::lock_guard<std::mutex> lock(mutex_);
    pools_[key] = std::move(ep);
    std::cout << "ConnectionPool: created pool for " << key
              << " with " << poolSize << " connections" << std::endl;
    return true;
}

ConnectionPool::TcpClientPtr ConnectionPool::acquire(const std::string& host, uint16_t port) {
    std::string key = makeKey(host, port);

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pools_.find(key);
    if (it == pools_.end()) {
        return nullptr;
    }

    auto& ep = it->second;
    size_t idx = ep->nextIndex.fetch_add(1, std::memory_order_relaxed) % ep->clients.size();
    return ep->clients[idx];
}

int ConnectionPool::poolSize(const std::string& host, uint16_t port) const {
    std::string key = makeKey(host, port);

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pools_.find(key);
    if (it == pools_.end()) {
        return 0;
    }
    return static_cast<int>(it->second->clients.size());
}

} // namespace rpc
