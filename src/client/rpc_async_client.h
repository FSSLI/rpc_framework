// src/client/rpc_async_client.h
#ifndef RPC_ASYNC_CLIENT_H
#define RPC_ASYNC_CLIENT_H

#include <string>
#include <memory>
#include <future>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <functional>
#include <random>

#include "network/tcp_client.h"
#include "network/tcp_connection.h"
#include "network/event_loop.h"
#include "network/event_loop_thread.h"
#include "codec/rpc_codec.h"
#include "protocol/rpc_service.pb.h"
#include "discovery/service_registry.h"
#include "loadbalance/consistent_hash.h"
#include "interceptor/interceptor.h"

namespace rpc {

class ServiceRegistry;
class CircuitBreaker;
class TokenBucket;

enum class LBPolicy { RR, CONSISTENT_HASH };

class RpcAsyncClient {
public:
    using ResponseCallback = std::function<void(const RpcResponse&)>;
    using ResponseFuture = std::future<RpcResponse>;
    using ResponsePromise = std::promise<RpcResponse>;

    RpcAsyncClient(const std::string& host, uint16_t port);
    RpcAsyncClient(std::shared_ptr<ServiceRegistry> registry,
                   const std::string& serviceName);
    // 连接池模式：使用预建的 TcpClient（EventLoop 生命周期由池管理）
    explicit RpcAsyncClient(std::shared_ptr<TcpClient> tcpClient);
    ~RpcAsyncClient();

    bool connect();
    void disconnect();

    // FIX: 增加 timeout_ms 参数，默认 5000ms
    ResponseFuture asyncCall(const std::string& service_name,
                             const std::string& method_name,
                             const RpcRequest& request,
                             int timeout_ms = 5000);

    void asyncCall(const std::string& service_name,
                   const std::string& method_name,
                   const RpcRequest& request,
                   ResponseCallback cb,
                   int timeout_ms = 5000);

    bool connected() const;
    void startHeartbeat(double intervalSeconds = 30.0);
    void stopHeartbeat();

    // 熔断器（可选，兼容旧接口）
    void setCircuitBreaker(CircuitBreaker* cb);
    // 限流器（可选，兼容旧接口）
    void setRateLimiter(TokenBucket* limiter);
    // 拦截器链（新接口：插入自定义拦截器）
    void addInterceptor(std::shared_ptr<Interceptor> interceptor) {
        chain_.addInterceptor(std::move(interceptor));
    }
    // 负载均衡策略（默认轮询）
    void setLBPolicy(LBPolicy p, int virtualNodes = 150);

private:
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t);
    void onWriteComplete(const TcpConnectionPtr& conn);

    void sendRequest(uint64_t req_id, const std::string& packet, const std::string& hashKey = "");
    std::shared_ptr<TcpClient> getTcpClient(const std::string& hashKey = "");
    RpcResponse doCall(const std::string& svc, const std::string& method,
                       RpcRequest& req, uint64_t reqId, int timeout);
    void handleResponse(const DecodedPacket& packet);
    void handleTimeout(uint64_t req_id);  // FIX: 超时处理

    void setupCallbacks();  // 提取公共回调设置（直接模式 + 池模式共用）
    bool resolveEndpoint();
    bool connectDirect();
    bool connectViaRegistry();

    std::string host_;
    uint16_t port_;

    std::shared_ptr<ServiceRegistry> registry_;
    std::string serviceName_;

    // 多节点负载均衡：每个节点独立 EventLoopThread + TcpClient
    struct Endpoint {
        std::string host;
        uint16_t port;
        std::shared_ptr<TcpClient> tcpClient;
        std::unique_ptr<EventLoopThread> loopThread;
    };
    std::vector<Endpoint> endpoints_;
    std::atomic<size_t> rrIndex_{0};  // RR 索引
    LBPolicy lbPolicy_ = LBPolicy::RR;
    std::unique_ptr<ConsistentHash> consistentHash_;  // unique_ptr（含 mutex 不可拷贝）

    EventLoop* loop_;  // 单节点模式使用；多节点模式置 nullptr
    std::unique_ptr<EventLoopThread> loopThread_;
    bool ownsLoopThread_ = true;  // 直接模式拥有线程，池/多节点模式不拥有
    // Issue #8 fix: shared_ptr 支持 sendRequest 安全捕获副本，消除数据竞争
    std::shared_ptr<TcpClient> tcpClient_;  // 单节点/池模式使用
    std::atomic<bool> connected_;
    std::atomic<bool> disconnecting_{false};  // Issue #2 fix: 防止 sendRequest 与 disconnect 数据竞争
    std::atomic<bool> destroyed_{false};   // Issue #4 fix: 防止析构后定时器回调 UAF

    std::atomic<uint64_t> nextReqId_;

    std::mutex pendingMutex_;
    std::unordered_map<uint64_t, ResponsePromise> pendingPromises_;
    std::unordered_map<uint64_t, ResponseCallback> pendingCallbacks_;

    std::atomic<bool> heartbeatRunning_{false};
    uint64_t heartbeatTimerId_ = 0;
    EventLoop* heartbeatLoop_ = nullptr;  // 心跳注册的 loop（可能与 loop_ 不同）

    // 拦截器链（新架构）
    InterceptorChain chain_;

    // 熔断器 + 限流器（不拥有所有权，兼容旧接口）
    CircuitBreaker* circuitBreaker_ = nullptr;
    TokenBucket* rateLimiter_ = nullptr;
};

} // namespace rpc

#endif