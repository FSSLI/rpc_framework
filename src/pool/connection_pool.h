// src/pool/connection_pool.h
#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <netinet/in.h>

namespace rpc {

class TcpClient;
class EventLoopThread;

class ConnectionPool {
public:
    using TcpClientPtr = std::shared_ptr<TcpClient>;

    ConnectionPool() = default;
    ~ConnectionPool();

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    // 为指定端点创建固定大小的连接池，所有连接预建立
    // 返回 false 表示有连接未能成功建立
    bool createPool(const std::string& host, uint16_t port, int poolSize);

    // 轮询获取一个 TcpClient，调用者不需释放
    TcpClientPtr acquire(const std::string& host, uint16_t port);

    // 获取池中客户端数量
    int poolSize(const std::string& host, uint16_t port) const;

private:
    struct EndpointPool {
        std::vector<TcpClientPtr> clients;
        std::vector<std::unique_ptr<EventLoopThread>> loops;
        std::atomic<size_t> nextIndex{0};
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<EndpointPool>> pools_;

    static std::string makeKey(const std::string& host, uint16_t port);
};

} // namespace rpc

#endif
