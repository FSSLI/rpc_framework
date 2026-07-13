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

namespace rpc {

class ServiceRegistry;

class RpcAsyncClient {
public:
    using ResponseCallback = std::function<void(const RpcResponse&)>;
    using ResponseFuture = std::future<RpcResponse>;
    using ResponsePromise = std::promise<RpcResponse>;

    RpcAsyncClient(const std::string& host, uint16_t port);
    RpcAsyncClient(std::shared_ptr<ServiceRegistry> registry,
                   const std::string& serviceName);
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

private:
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t);
    void onWriteComplete(const TcpConnectionPtr& conn);

    void sendRequest(uint64_t req_id, const std::string& packet);
    void handleResponse(const DecodedPacket& packet);
    void handleTimeout(uint64_t req_id);  // FIX: 超时处理

    bool resolveEndpoint();
    bool connectDirect();
    bool connectViaRegistry();

    std::string host_;
    uint16_t port_;

    std::shared_ptr<ServiceRegistry> registry_;
    std::string serviceName_;

    EventLoop* loop_;
    std::unique_ptr<EventLoopThread> loopThread_;
    // Issue #8 fix: shared_ptr 支持 sendRequest 安全捕获副本，消除数据竞争
    std::shared_ptr<TcpClient> tcpClient_;
    std::atomic<bool> connected_;
    std::atomic<bool> disconnecting_{false};  // Issue #2 fix: 防止 sendRequest 与 disconnect 数据竞争

    std::atomic<uint64_t> nextReqId_;

    std::mutex pendingMutex_;
    std::unordered_map<uint64_t, ResponsePromise> pendingPromises_;
    std::unordered_map<uint64_t, ResponseCallback> pendingCallbacks_;

    std::atomic<bool> heartbeatRunning_{false};
    uint64_t heartbeatTimerId_ = 0;  // FIX: 记录定时器ID用于取消
};

} // namespace rpc

#endif