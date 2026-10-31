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
    // 依赖 EndpointPool 成员析构顺序（reverse decl order）:
    // nextIndex → clients → loops
    // TcpClient 析构时 EventLoopThread 仍存活，~TcpClient() 可安全调用
    // connector_->stop() → queueInLoop 到仍在运行的 EventLoop
    for (auto& pair : pools_) {
        auto& ep = pair.second;
        for (auto& client : ep->clients) {
            if (client) {
                client->stop();  // 仅停止 Connector 自动重连
            }
        }
        // clients 和 loops 由 EndpointPool 自然析构:
        // clients 先释放 → ~TcpClient() 安全（loops 仍存活）
        // loops 后释放 → ~EventLoopThread() stop+join
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
        auto loopThread = std::make_unique<EventLoopThread>();
        EventLoop* loop = loopThread->startLoop();

        auto client = std::make_shared<TcpClient>(loop, serverAddr);
        client->setRetryOnDisconnect(true);

        // Issue #2 fix: shared_ptr 管理，防止栈变量被回调悬空引用
        auto promisePtr = std::make_shared<std::promise<bool>>();
        auto promiseSetPtr = std::make_shared<std::atomic<bool>>(false);
        auto connectFuture = promisePtr->get_future();

        client->setConnectionCallback(
            [promisePtr, promiseSetPtr](const TcpConnectionPtr& conn) {
                bool expected = false;
                if (promiseSetPtr->compare_exchange_strong(expected, true)) {
                    promisePtr->set_value(conn->connected());
                }
            });

        client->connect();

        auto status = connectFuture.wait_for(std::chrono::milliseconds(3000));
        if (status == std::future_status::timeout) {
            bool expected = false;
            if (promiseSetPtr->compare_exchange_strong(expected, true)) {
                promisePtr->set_value(false);
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

        // 清除 pool 创建时的 promise 回调，防止后续断连时访问已释放的 shared_ptr
        client->setConnectionCallback({});
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
    const size_t n = ep->clients.size();
    TcpClientPtr fallback;
    // 轮询查找"健康且未被其他客户端绑定"的连接
    for (size_t attempt = 0; attempt < n; ++attempt) {
        size_t idx = ep->nextIndex.fetch_add(1, std::memory_order_relaxed) % n;
        auto& client = ep->clients[idx];
        if (client->isClaimed()) continue;   // 已被别的 RpcAsyncClient 占用
        auto conn = client->connection();
        if (conn && conn->connected()) return client;
        if (!fallback) fallback = client;    // 未占用但断开，作为兜底（它会自动重连）
    }
    // 全部被占用时返回 nullptr，而不是把已占用的连接交出去（那会互相覆盖回调）
    if (fallback) fallback->ensureConnected();  // 兜底的断线连接：主动触发一次重连
    return fallback;
}

void ConnectionPool::release(const TcpClientPtr& client) {
    if (!client) return;

    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& pair : pools_) {
        auto& ep = pair.second;
        for (auto& c : ep->clients) {
            if (c.get() != client.get()) continue;

            if (client->isClaimed()) {
                std::cerr << "ConnectionPool::release: 该连接仍被客户端占用，"
                             "请先析构 RpcAsyncClient 再归还" << std::endl;
                return;
            }
            client->clearCallbacks();
            client->ensureConnected();
            return;
        }
    }
    std::cerr << "ConnectionPool::release: 该连接不属于任何池" << std::endl;
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
