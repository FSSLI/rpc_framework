// src/client/inline_pipeline_client.h
//
// 单线程流水线客户端：调用线程自己发、自己收、自己判超时。
//
// 与其它两个客户端的关系：
//   RpcAsyncClient       —— 跨线程投递 + 回调/Future；每请求 1 次 eventfd（send）+ 1 次（定时器）
//   InlineSyncClient     —— 调用线程驱动 loop，但**一次只有一个在途请求**
//   InlinePipelineClient —— 调用线程驱动 loop + **多个在途请求**（本文件）
//
// 关键设计：
//   1. EventLoop 构造在调用线程 → send 直接 write()，不需要 eventfd 唤醒；
//   2. pending 表只被本线程访问 → **不需要锁**；
//   3. 超时不用每请求定时器，而是把"最近的 deadline"交给 epoll_wait，
//      每轮 pump 后扫描一遍（O(在途数)，在途数很小）；
//   4. 没有 promise / futex，结果通过回调直接在 pump 的调用栈里返回。
//
// 约束：实例必须在**使用它的线程**里构造和销毁；线程私有。
#ifndef RPC_INLINE_PIPELINE_CLIENT_H
#define RPC_INLINE_PIPELINE_CLIENT_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "codec/rpc_codec.h"
#include "network/buffer.h"
#include "network/event_loop.h"
#include "network/tcp_client.h"
#include "network/tcp_connection.h"
#include "protocol/rpc_service.pb.h"

namespace rpc {

class InlinePipelineClient {
public:
    using Clock = std::chrono::steady_clock;
    // 完成回调：响应 + 本次请求耗时（微秒）
    using CompletionCallback = std::function<void(const RpcResponse&, double latencyUs)>;

    InlinePipelineClient(const std::string& host, uint16_t port);
    ~InlinePipelineClient();

    InlinePipelineClient(const InlinePipelineClient&) = delete;
    InlinePipelineClient& operator=(const InlinePipelineClient&) = delete;

    bool connect(int timeoutMs = 3000);
    bool connected() const;

    // 提交一个请求（不等待结果）。返回 false 表示当前没有可用连接或编码失败。
    bool submit(const std::string& service,
                const std::string& method,
                const RpcRequest& request,
                int timeoutMs,
                CompletionCallback cb);

    // 驱动一轮事件循环：epoll_wait + 解码 + 回调 + 超时扫描。
    // 返回值：本轮完成的请求数（含超时）。
    int pumpOnce();

    size_t pending() const { return pending_.size(); }

private:
    struct Pending {
        uint64_t id;
        Clock::time_point sentAt;
        Clock::time_point deadline;
        CompletionCallback cb;
    };

    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t receiveTime);

    int sweepTimeouts();   // 处理已过期请求，返回个数
    int nextWaitMs() const;  // 距离最近 deadline 的毫秒数（无在途时返回默认值）

    EventLoop loop_;                       // 构造于调用线程
    std::shared_ptr<TcpClient> client_;
    std::unordered_map<uint64_t, Pending> pending_;  // 线程私有，无需加锁
    uint64_t nextId_ = 1;
    int completions_ = 0;                  // 本轮 pumpOnce 完成的请求数
    bool valid_ = false;
};

}  // namespace rpc

#endif  // RPC_INLINE_PIPELINE_CLIENT_H
