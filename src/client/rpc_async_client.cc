// src/client/rpc_async_client.cc
// ============================================================================
// 异步 RPC 客户端实现
// 核心逻辑：连接管理 + 请求发送 + 响应分发 + 心跳保活
// ============================================================================

#include "rpc_async_client.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

namespace rpc {

RpcAsyncClient::RpcAsyncClient(const std::string& host, uint16_t port)
    : host_(host), port_(port), loop_(nullptr), connected_(false), nextReqId_(1) {
    // nextReqId_ 初始化为 1，0 保留作特殊用途（如心跳包可固定用 0）
}

RpcAsyncClient::~RpcAsyncClient() {
    disconnect();
}

// ============================================================================
// connect() - 建立连接（阻塞接口，内部异步实现）
// ============================================================================
// 设计思路：
// 1. 创建独立 IO 线程（EventLoopThread），专门处理网络事件
// 2. 在 IO 线程里执行 socket 创建和 connect（线程安全）
// 3. 用 promise/future 阻塞等待连接结果，对外表现为同步接口
// 
// 为什么不在构造函数里连接？
// - 连接可能失败，构造函数无法返回错误码
// - 支持断线重连：先 disconnect 再 connect
// ============================================================================
bool RpcAsyncClient::connect() {
    // Step 1: 创建 IO 线程，启动 EventLoop
    loopThread_ = std::make_unique<EventLoopThread>();
    loop_ = loopThread_->startLoop();  // 阻塞直到 IO 线程启动完成

    // Step 2: 在 IO 线程里创建 socket 并连接
    // 使用 promise/future 跨线程传递连接结果
    std::promise<bool> connectPromise;
    auto connectFuture = connectPromise.get_future();

    loop_->runInLoop([this, &connectPromise]() {
        // 创建非阻塞 socket（SOCK_NONBLOCK）+ 关闭 exec（SOCK_CLOEXEC）
        // SOCK_NONBLOCK：connect 立即返回，不会阻塞 IO 线程
        // SOCK_CLOEXEC：fork 子进程时自动关闭，防止 fd 泄漏
        int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (sockfd < 0) {
            connectPromise.set_value(false);
            return;
        }

        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(port_);
        inet_pton(AF_INET, host_.c_str(), &serverAddr.sin_addr);

        // 非阻塞 connect：立即返回，errno == EINPROGRESS 表示正在连接
        int ret = ::connect(sockfd, reinterpret_cast<struct sockaddr*>(&serverAddr), sizeof(serverAddr));
        if (ret < 0 && errno != EINPROGRESS) {
            ::close(sockfd);
            connectPromise.set_value(false);
            return;
        }

        // 获取本地地址，用于创建 TcpConnection
        struct sockaddr_in localAddr;
        memset(&localAddr, 0, sizeof(localAddr));
        socklen_t addrlen = sizeof(localAddr);
        getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&localAddr), &addrlen);

        // 创建 TcpConnection 对象，绑定到 IO 线程的 EventLoop
        connection_.reset(new TcpConnection(loop_, "client", sockfd, localAddr, serverAddr));

        // ========================================================================
        // 设置连接建立回调
        // ========================================================================
        // 技巧：用 getContext() 判空来区分"首次建立"和"重连"
        // - 首次建立：context 为 nullptr，设置 connected_ = true
        // - 重连：context 已设置，不再重复通知
        // 
        // 为什么用 context 而不是状态变量？
        // - TcpConnection 本身不维护"是否已回调"状态
        // - context 是用户自定义上下文，适合这种标记用途
        // ========================================================================
        connection_->setConnectionCallback(
            [this, &connectPromise](const TcpConnectionPtr& conn) {
                if (conn->getContext() == nullptr) {
                    connected_ = true;
                    conn->setContext(reinterpret_cast<void*>(1));  // 标记已连接
                    connectPromise.set_value(true);
                }
            });

        // 设置消息到达回调：收到数据时触发解码和响应分发
        connection_->setMessageCallback(
            std::bind(&RpcAsyncClient::onMessage, this, 
                      std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

        // 设置写完成回调：可用于流量控制（如背压）
        connection_->setWriteCompleteCallback(
            std::bind(&RpcAsyncClient::onWriteComplete, this, std::placeholders::_1));

        // 设置连接关闭回调：清理状态，为重连做准备
        connection_->setCloseCallback(
            [this](const TcpConnectionPtr& conn) {
                connected_ = false;
                // TODO: 连接断开时应清理 pendingPromises_ 和 pendingCallbacks_
                // 通知所有等待中的请求"连接已断开"
            });

        // 注册到 EventLoop，开始监听可读/可写事件
        connection_->connectEstablished();
    });

    // 阻塞等待连接结果（超时控制由调用方实现）
    return connectFuture.get();
}

// ============================================================================
// disconnect() - 断开连接
// ============================================================================
// 注意顺序：
// 1. 先在 IO 线程销毁 TcpConnection（取消事件监听）
// 2. 再停止 IO 线程（join）
// 3. 最后标记状态
// 顺序不能反，否则可能访问已销毁的资源
// ============================================================================
void RpcAsyncClient::disconnect() {
    if (loop_) {
        loop_->runInLoop([this]() {
            if (connection_) {
                connection_->connectDestroyed();
                connection_.reset();
            }
        });
    }
    if (loopThread_) {
        loopThread_->stop();
        loopThread_.reset();
    }
    connected_ = false;
    loop_ = nullptr;
}

// ============================================================================
// asyncCall - Future 模式
// ============================================================================
// 执行流程：
// 1. 生成唯一 req_id
// 2. 创建 promise，将 future 返回给调用方
// 3. promise 存入 pendingPromises_（IO 线程会在响应到达时 set_value）
// 4. 序列化请求并发送
// 
// 线程安全：
// - nextReqId_ 是 atomic，无需加锁
// - pendingPromises_ 需要 mutex 保护（业务线程写，IO 线程读/删）
// 
// 潜在问题：
// - promise 被 move 进 map 后，如果连接断开，promise 永远不被 set_value
// - 需要超时机制：定时器超时时从 map 移除并 set_exception
// ============================================================================
RpcAsyncClient::ResponseFuture RpcAsyncClient::asyncCall(
    const std::string& service_name,
    const std::string& method_name,
    const RpcRequest& request) {

    uint64_t reqId = nextReqId_.fetch_add(1);

    ResponsePromise promise;
    ResponseFuture future = promise.get_future();

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingPromises_[reqId] = std::move(promise);
    }

    // 编码：Protobuf → 二进制包
    std::string packet = Codec::encodeRequest(request, reqId, service_name, method_name);
    // 加长度头：解决粘包问题（Length-Field 协议）
    std::string packetWithLen = Codec::encodeWithLength(packet);

    sendRequest(reqId, packetWithLen);

    return future;
}

// ============================================================================
// asyncCall - Callback 模式
// ============================================================================
// 与 Future 模式的区别：
// - 调用方传入 lambda/function，响应到达时直接调用
// - 不需要等待，不需要 get()，纯异步链式处理
// 
// 适用场景：
// - RPC 网关：收到 HTTP 请求 → 异步 RPC → 收到响应 → 回写 HTTP 响应
// - 批量请求：发 100 个请求，每个有独立回调处理结果
// ============================================================================
void RpcAsyncClient::asyncCall(
    const std::string& service_name,
    const std::string& method_name,
    const RpcRequest& request,
    ResponseCallback cb) {

    uint64_t reqId = nextReqId_.fetch_add(1);

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingCallbacks_[reqId] = std::move(cb);
    }

    std::string packet = Codec::encodeRequest(request, reqId, service_name, method_name);
    std::string packetWithLen = Codec::encodeWithLength(packet);

    sendRequest(reqId, packetWithLen);
}

// ============================================================================
// sendRequest - 线程安全的发送
// ============================================================================
// 关键问题：asyncCall 可能在任意线程调用（业务线程），
// 但 TcpConnection::send 必须在 IO 线程执行（非线程安全）
// 
// 解决方案：
// - 如果当前在 IO 线程，直接发送
// - 否则，通过 runInLoop 将任务投递到 IO 线程队列
// 
// 为什么 send 不线程安全？
// - TcpConnection 内部有 outputBuffer，多线程同时写会数据错乱
// - EventLoop 保证单线程执行，天然线程安全
// ============================================================================
void RpcAsyncClient::sendRequest(uint64_t req_id, const std::string& packet) {
    if (loop_->isInLoopThread()) {
        connection_->send(packet);
    } else {
        loop_->runInLoop([this, packet]() {
            connection_->send(packet);
        });
    }
}

void RpcAsyncClient::onConnection(const TcpConnectionPtr& conn) {
    // 连接建立逻辑已在 setConnectionCallback 的 lambda 中处理
    // 这里留空是因为 TcpConnection 要求设置回调，但实际逻辑在 connect() 里
}

// ============================================================================
// onMessage - 消息到达回调（IO 线程）
// ============================================================================
// 处理流程：
// 1. 从 Buffer 中按 Length-Field 协议拆包（解决粘包/拆包）
// 2. 对每个完整包，解码 Protobuf 得到 DecodedPacket
// 3. 根据 msg_type 分发：RESPONSE → handleResponse
// 
// 为什么是 while 循环？
// - TCP 是流式协议，一次可读事件可能收到多个完整包
// - 必须循环处理直到 Buffer 中不足一个完整包
// ============================================================================
void RpcAsyncClient::onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    std::string packet;
    // decodeWithLength：从 buf 中读取 4 字节长度头 + 对应长度的数据
    while (Codec::decodeWithLength(*buf, packet)) {
        Buffer rpcBuf;
        rpcBuf.append(packet.data(), packet.size());

        DecodedPacket decoded;
        if (!Codec::decode(rpcBuf, decoded)) {
            // 解码失败：可能是协议版本不匹配或数据损坏
            // 生产环境应记录日志、关闭连接、上报监控
            continue;
        }

        if (decoded.msg_type == MsgType::RESPONSE) {
            handleResponse(decoded);
        }
        // TODO: 处理 HEARTBEAT_ACK 等消息类型
    }
}

// ============================================================================
// handleResponse - 响应分发（IO 线程）
// ============================================================================
// 核心逻辑：根据 req_id 从 pending 表找到对应的 promise 或 callback
// 
// 注意事项：
// 1. 为什么先查 promise 再查 callback？
//    - 两种模式互斥，一个 req_id 只会出现在一个 map 中
//    - 顺序无所谓，但通常 Future 模式更常用，放前面减少比较次数
// 
// 2. 为什么用 lock_guard 而不是 unique_lock？
//    - 操作简单，不需要条件变量，lock_guard 更轻量
// 
// 3. 为什么 erase 后立即释放锁？
//    - promise.set_value 和 callback 执行可能耗时（用户逻辑）
//    - 持有锁期间执行用户代码会阻塞其他响应处理
//    - 但当前实现是先 set_value/callback 再 erase，可以优化
// 
// 优化建议：
// - 先 extract（C++17）或 find + 复制指针，然后 unlock，再执行回调
// - 避免持有锁期间执行用户代码
// ============================================================================
void RpcAsyncClient::handleResponse(const DecodedPacket& packet) {
    uint64_t reqId = packet.req_id;

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);

        // 尝试 Future 模式
        auto promiseIt = pendingPromises_.find(reqId);
        if (promiseIt != pendingPromises_.end()) {
            promiseIt->second.set_value(packet.rpc_response);
            pendingPromises_.erase(promiseIt);
            return;  // 找到即返回，一个 req_id 只对应一种模式
        }

        // 尝试 Callback 模式
        auto callbackIt = pendingCallbacks_.find(reqId);
        if (callbackIt != pendingCallbacks_.end()) {
            callbackIt->second(packet.rpc_response);
            pendingCallbacks_.erase(callbackIt);
        }
    }
}

void RpcAsyncClient::onWriteComplete(const TcpConnectionPtr& conn) {
    // 写完成回调触发时机：outputBuffer 全部写入内核发送缓冲区
    // 可用于：
    // 1. 流量控制（背压）：发送太快时暂停，等写完成再继续
    // 2. 批量发送确认：确保一批请求全部发出后再发下一批
    // 3. 性能统计：记录发送延迟
}

bool RpcAsyncClient::connected() const {
    return connected_;
}

// ============================================================================
// startHeartbeat - 心跳保活
// ============================================================================
// 设计背景：
// - 服务端有 idle 超时检测（如 60s 无数据则断开）
// - 客户端长时间不发请求会被服务端踢掉
// - 心跳包维持连接活性，避免无辜断开
// 
// 实现要点：
// 1. 心跳在 IO 线程定时发送（利用 EventLoop 的定时器）
// 2. 间隔应小于服务端 idle 超时（如服务端 60s，客户端 30s）
// 3. 连接断开时自动停止（通过 connected_ 判断）
// 
// 进阶：
// - 心跳应答检测：发心跳后等 ack，超时未收到认为连接异常
// - 心跳带负载信息：服务端可通过心跳做服务发现
// ============================================================================
void RpcAsyncClient::startHeartbeat(double intervalSeconds) {
    // exchange 保证只有一个线程成功启动心跳
    if (loop_ && !heartbeatRunning_.exchange(true)) {
        loop_->runInLoop([this, intervalSeconds]() {
            // runEvery：每隔 intervalSeconds 执行一次回调
            loop_->runEvery(intervalSeconds, [this]() {
                if (connected_ && connection_) {
                    rpc::Heartbeat hb;
                    hb.set_service_name("client");
                    hb.set_node_id("node_1");
                    hb.set_timestamp(std::time(nullptr));

                    std::string packet = Codec::encodeHeartbeat(hb);
                    std::string packetWithLen = Codec::encodeWithLength(packet);
                    connection_->send(packetWithLen);
                }
            });
        });
    }
}

void RpcAsyncClient::stopHeartbeat() {
    heartbeatRunning_ = false;
    // TODO: 需要 EventLoop 支持取消定时器
    // 当前简化版：标记停止，但已注册的定时器仍会执行（通过 connected_ 判断跳过）
}

} // namespace rpc