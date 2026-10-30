// src/client/inline_sync_client.h
//
// 同步 RPC 客户端：由**调用线程自己驱动 IO**（不为连接创建 IO 线程）。
//
// 与 RpcAsyncClient + Future 的区别（每次调用的线程开销）：
//   RpcAsyncClient（跨线程）:
//     worker --eventfd--> IO 线程 --write--> 服务端
//     worker <--futex---- IO 线程 <--read--- 服务端
//     = 2 次线程切换 + 1 次 eventfd 写 + 1 次 futex 唤醒
//
//   InlineSyncClient（本线程驱动）:
//     worker(== loop 线程) --write--> 服务端
//     worker(== loop 线程) <--read--- 服务端
//     = 0 次线程切换、0 次 eventfd、0 次 futex
//
// 约束：
//   1. 实例必须在**使用它的线程**中构造和销毁（EventLoop 有线程亲和性）；
//   2. 同一实例串行调用（一次只有一个在途请求）；
//   3. 一个实例一条连接，一个线程一个 EventLoop。
#ifndef RPC_INLINE_SYNC_CLIENT_H
#define RPC_INLINE_SYNC_CLIENT_H

#include <cstdint>
#include <memory>
#include <string>

#include "codec/rpc_codec.h"
#include "network/buffer.h"
#include "network/event_loop.h"
#include "network/tcp_client.h"
#include "network/tcp_connection.h"
#include "protocol/rpc_service.pb.h"

namespace rpc {

class InlineSyncClient {
public:
    InlineSyncClient(const std::string& host, uint16_t port);
    ~InlineSyncClient();

    InlineSyncClient(const InlineSyncClient&) = delete;
    InlineSyncClient& operator=(const InlineSyncClient&) = delete;

    // 建连并驱动事件循环直到连接成功或超时
    bool connect(int timeoutMs = 3000);
    bool connected() const;

    // 同步调用：发送后在本线程反复 loopOnce 直到收到响应或超时
    RpcResponse call(const std::string& service,
                     const std::string& method,
                     const RpcRequest& request,
                     int timeoutMs = 3000);

private:
    void onConnection(const TcpConnectionPtr& conn);
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t receiveTime);

    EventLoop loop_;                      // 构造于调用线程
    std::shared_ptr<TcpClient> client_;
    bool valid_ = false;

    uint64_t nextId_ = 1;
    uint64_t pendingId_ = 0;
    bool done_ = false;
    RpcResponse result_;

    bool connectDone_ = false;
    bool connectOk_ = false;
};

}  // namespace rpc

#endif  // RPC_INLINE_SYNC_CLIENT_H
