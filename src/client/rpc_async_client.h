// src/client/rpc_async_client.h
// ============================================================================
// 异步 RPC 客户端头文件
// 设计目标：支持 Future/Promise 和 Callback 两种异步调用模式
// 核心机制：req_id 关联请求-响应，独立 IO 线程处理网络事件
// ============================================================================

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
    // ------------------------------------------------------------------------
    // 类型别名：两种异步回调风格
    // ------------------------------------------------------------------------
    using ResponseCallback = std::function<void(const RpcResponse&)>;
    using ResponseFuture = std::future<RpcResponse>;
    using ResponsePromise = std::promise<RpcResponse>;

    RpcAsyncClient(const std::string& host, uint16_t port);
    ~RpcAsyncClient();

    // 连接服务器（阻塞等待连接建立）
    bool connect();
    void disconnect();

    // ========================================================================
    // 异步调用 - 双模式设计
    // ========================================================================

    /**
     * 模式一：Future 模式
     * 调用方获得 future，自行决定何时 get() 获取结果
     * 适用场景：需要同步等待结果，但不想阻塞发送线程
     * 
     * 示例：
     *   auto future = client.asyncCall("UserService", "GetUser", req);
     *   // ... 做其他事 ...
     *   auto resp = future.get();  // 这里阻塞等待响应
     */
    ResponseFuture asyncCall(const std::string& service_name,
                             const std::string& method_name,
                             const RpcRequest& request);

    /**
     * 模式二：Callback 模式
     * 调用方传入回调函数，响应到达时自动触发
     * 适用场景：纯异步链式处理，如 RPC 网关、异步流水线
     * 
     * 示例：
     *   client.asyncCall("UserService", "GetUser", req, 
     *     [](const RpcResponse& resp) {
     *       processResponse(resp);
     *     });
     */
    void asyncCall(const std::string& service_name,
                   const std::string& method_name,
                   const RpcRequest& request,
                   ResponseCallback cb);

    bool connected() const;

    // 心跳保活：定时发送心跳包，防止服务端 idle 超时断开
    void startHeartbeat(double intervalSeconds = 30.0);
    void stopHeartbeat();

private:
    // ------------------------------------------------------------------------
    // TcpConnection 回调（由 EventLoop 触发）
    // ------------------------------------------------------------------------
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t);
    void onWriteComplete(const TcpConnectionPtr& conn);

    // 发送请求 + 处理响应
    void sendRequest(uint64_t req_id, const std::string& packet);
    void handleResponse(const DecodedPacket& packet);

    std::string host_;
    uint16_t port_;

    // ========================================================================
    // IO 线程模型：One Loop Per Thread
    // ========================================================================
    // 客户端必须有自己的 EventLoop，原因：
    // 1. 响应是服务端"推"过来的，需要事件循环监听可读事件
    // 2. 定时器（心跳、超时）依赖 EventLoop 的 timerfd
    // 3. 所有网络操作必须在 IO 线程执行，避免多线程竞争
    // 
    // 对比同步客户端：同步客户端发完请求就阻塞等响应，不需要事件循环
    // ========================================================================
    EventLoop* loop_;                          // IO 线程的 EventLoop 指针
    std::unique_ptr<EventLoopThread> loopThread_;  // 独立 IO 线程

    TcpConnectionPtr connection_;              // TCP 连接对象
    std::atomic<bool> connected_;              // 连接状态（跨线程安全）

    // ========================================================================
    // 请求-响应关联机制
    // ========================================================================
    // 问题：异步场景下，多个请求并发发送，如何知道哪个响应对应哪个请求？
    // 解决：每个请求分配唯一 req_id，服务端原样返回，客户端按 req_id 匹配
    // 
    // req_id 生成策略：原子自增（atomic fetch_add）
    // - 线程安全：无需加锁
    // - 唯一性：单客户端生命周期内不重复（uint64_t 足够大，不会溢出）
    // ========================================================================
    std::atomic<uint64_t> nextReqId_;

    // ========================================================================
    // pending 表：等待中的请求
    // ========================================================================
    // 设计要点：
    // 1. 为什么用 unordered_map？O(1) 查找，响应到达时快速定位
    // 2. 为什么加 mutex？asyncCall（业务线程）和 handleResponse（IO 线程）并发访问
    // 3. 为什么分两个 map？Future 和 Callback 两种模式独立存储，互不干扰
    // 
    // 内存安全：连接断开时必须清理 pending，否则 promise/callback 永远挂起
    // ========================================================================
    std::mutex pendingMutex_;
    std::unordered_map<uint64_t, ResponsePromise> pendingPromises_;   // Future 模式
    std::unordered_map<uint64_t, ResponseCallback> pendingCallbacks_; // Callback 模式

    std::atomic<bool> heartbeatRunning_{false};
};

} // namespace rpc

#endif