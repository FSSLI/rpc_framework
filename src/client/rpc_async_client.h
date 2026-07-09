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

#include "network/tcp_connection.h"
#include "network/event_loop.h"
#include "network/event_loop_thread.h"
#include "codec/rpc_codec.h"
#include "protocol/rpc_service.pb.h"

namespace rpc {

class RpcAsyncClient {
public:
    using ResponseCallback = std::function<void(const RpcResponse&)>;
    using ResponseFuture = std::future<RpcResponse>;
    using ResponsePromise = std::promise<RpcResponse>;

    RpcAsyncClient(const std::string& host, uint16_t port);
    ~RpcAsyncClient();

    // 连接服务器
    bool connect();
    void disconnect();

    // 异步调用，返回 future
    ResponseFuture asyncCall(const std::string& service_name,
                             const std::string& method_name,
                             const RpcRequest& request);

    // 异步调用，带回调
    void asyncCall(const std::string& service_name,
                   const std::string& method_name,
                   const RpcRequest& request,
                   ResponseCallback cb);

    bool connected() const;

    void startHeartbeat(double intervalSeconds = 30.0);
    void stopHeartbeat();

private:
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t);
    void onWriteComplete(const TcpConnectionPtr& conn);

    void sendRequest(uint64_t req_id, const std::string& packet);
    void handleResponse(const DecodedPacket& packet);

    std::string host_;
    uint16_t port_;

    EventLoop* loop_;  // IO线程的EventLoop
    std::unique_ptr<EventLoopThread> loopThread_;
    TcpConnectionPtr connection_;
    std::atomic<bool> connected_;

    std::atomic<uint64_t> nextReqId_;
    
    // pending calls
    std::mutex pendingMutex_;
    std::unordered_map<uint64_t, ResponsePromise> pendingPromises_;
    std::unordered_map<uint64_t, ResponseCallback> pendingCallbacks_;

    // 新增
    std::atomic<bool> heartbeatRunning_{false};
};

} // namespace rpc

#endif