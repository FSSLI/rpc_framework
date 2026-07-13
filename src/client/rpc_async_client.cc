// src/client/rpc_async_client.cc
#include "client/rpc_async_client.h"
#include "discovery/service_registry.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <random>
#include <algorithm>  // std::shuffle

namespace rpc {

// ============================================================================
// 构造函数
// ============================================================================

RpcAsyncClient::RpcAsyncClient(const std::string& host, uint16_t port)
    : host_(host), port_(port), loop_(nullptr), connected_(false), nextReqId_(1) {
}

RpcAsyncClient::RpcAsyncClient(std::shared_ptr<ServiceRegistry> registry,
                                 const std::string& serviceName)
    : registry_(registry), serviceName_(serviceName),
      host_(), port_(0), loop_(nullptr), connected_(false), nextReqId_(1) {
}

RpcAsyncClient::~RpcAsyncClient() {
    // Issue #1 fix: 先停心跳定时器，再清理资源
    // 注意：disconnect() 必须在 lock_guard 之前调用。
    // disconnect() 内部会 join ioLoop 线程，如果 ioLoop 里有回调（如 handleResponse）
    // 正在等 pendingMutex_，先加锁再 join 会导致死锁。
    // 先 disconnect() 确保 ioLoop 已停止，再安全清理 pending 请求。
    stopHeartbeat();
    disconnect();

    // FIX: 清理所有 pending 请求，避免死等
    std::lock_guard<std::mutex> lock(pendingMutex_);

    RpcResponse errorResp;
    errorResp.set_success(false);
    errorResp.set_error_msg("client destroyed");

    for (auto& pair : pendingPromises_) {
        try {
            pair.second.set_value(errorResp);
        } catch (...) {
            // promise 可能已经被设置过，忽略异常
        }
    }
    pendingPromises_.clear();

    for (auto& pair : pendingCallbacks_) {
        try {
            pair.second(errorResp);
        } catch (...) {
            // 回调异常，忽略
        }
    }
    pendingCallbacks_.clear();
}

// ============================================================================
// 连接
// ============================================================================

bool RpcAsyncClient::connect() {
    if (registry_ && !serviceName_.empty()) {
        return connectViaRegistry();
    }
    return connectDirect();
}

bool RpcAsyncClient::connectViaRegistry() {
    auto nodes = registry_->discover(serviceName_);
    if (nodes.empty()) {
        std::cerr << "RpcAsyncClient: no available nodes for " << serviceName_ << std::endl;
        return false;
    }

    // 随机打乱节点顺序，避免所有客户端同时连同一个节点（thundering herd）
    static thread_local std::random_device rd;
    static thread_local std::mt19937 gen(rd());
    std::shuffle(nodes.begin(), nodes.end(), gen);

    for (const auto& node : nodes) {
        host_ = node.host;
        port_ = node.port;
        std::cout << "RpcAsyncClient: trying " << serviceName_
                  << " -> " << host_ << ":" << port_ << std::endl;

        if (connectDirect()) {
            return true;
        }
        // connectDirect() 失败时已自行清理 tcpClient_ 和 loopThread_，此处重置状态即可
        // FIX: 连接失败必须彻底清理，防止旧 Connector 后台重连
        disconnect();
        connected_ = false;
    }

    std::cerr << "RpcAsyncClient: all nodes exhausted for " << serviceName_ << std::endl;
    return false;
}

bool RpcAsyncClient::resolveEndpoint() {
    auto nodes = registry_->discover(serviceName_);
    if (nodes.empty()) {
        std::cerr << "RpcAsyncClient: no available nodes for " << serviceName_ << std::endl;
        return false;
    }

    // FIX: 使用 C++11 random 替代 srand/rand
    static thread_local std::random_device rd;
    static thread_local std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> dist(0, nodes.size() - 1);
    const auto& node = nodes[dist(gen)];

    host_ = node.host;
    port_ = node.port;

    std::cout << "RpcAsyncClient: resolved " << serviceName_
              << " -> " << host_ << ":" << port_ << std::endl;
    return true;
}

bool RpcAsyncClient::connectDirect() {
    loopThread_ = std::make_unique<EventLoopThread>();
    loop_ = loopThread_->startLoop();

    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port_);
    // Issue #8 fix: 检查 inet_pton 返回值，失败时立即退出
    if (inet_pton(AF_INET, host_.c_str(), &serverAddr.sin_addr) <= 0) {
        std::cerr << "RpcAsyncClient::connectDirect: invalid IP " << host_ << std::endl;
        loopThread_->stop();
        loopThread_.reset();
        loop_ = nullptr;
        return false;
    }

    tcpClient_ = std::make_shared<TcpClient>(loop_, serverAddr);

    // Issue #6 fix: 用 shared_ptr 管理 promise 和 atomic，避免栈变量被回调悬空引用
    auto promisePtr = std::make_shared<std::promise<bool>>();
    auto promiseSetPtr = std::make_shared<std::atomic<bool>>(false);
    auto connectFuture = promisePtr->get_future();

    tcpClient_->setConnectionCallback(
        [this, promisePtr, promiseSetPtr](const TcpConnectionPtr& conn) {
            if (conn->connected()) {
                connected_ = true;
                bool expected = false;
                if (promiseSetPtr->compare_exchange_strong(expected, true)) {
                    promisePtr->set_value(true);
                }
            } else {
                // Issue #1 fix: 连接断开时也通知 promise（如连接建立后立即断开）
                connected_ = false;
                bool expected = false;
                if (promiseSetPtr->compare_exchange_strong(expected, true)) {
                    promisePtr->set_value(false);
                }
            }
        });

    tcpClient_->setMessageCallback(
        std::bind(&RpcAsyncClient::onMessage, this,
                  std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

    tcpClient_->setWriteCompleteCallback(
        std::bind(&RpcAsyncClient::onWriteComplete, this, std::placeholders::_1));

    // 启动连接（Connector 异步 connect + 自动重连）
    tcpClient_->connect();

    // 等待首次连接结果，超时 3 秒
    auto status = connectFuture.wait_for(std::chrono::milliseconds(3000));
    if (status == std::future_status::timeout) {
        bool expected = false;
        if (promiseSetPtr->compare_exchange_strong(expected, true)) {
            promisePtr->set_value(false);
        }
        // Issue #3 fix: 先 join 线程确保无回调执行，再销毁 TcpClient，防止 UAF
        tcpClient_->stop();
        loopThread_->stop();
        tcpClient_.reset();
        loopThread_.reset();
        loop_ = nullptr;
        std::cerr << "RpcAsyncClient: initial connection timeout" << std::endl;
        return false;
    }

    bool ok = connectFuture.get();
    // Issue #5 fix + Issue #3: 先 join 线程再 reset 对象
    if (!ok || !connected_) {
        tcpClient_->stop();
        loopThread_->stop();
        tcpClient_.reset();
        loopThread_.reset();
        loop_ = nullptr;
        return false;
    }
    return true;
}

void RpcAsyncClient::disconnect() {
    // Issue #2 fix: 先设置标志，阻止新请求进入
    disconnecting_ = true;
    if (tcpClient_) {
        tcpClient_->disconnectPermanently();  // 停止重连 + 关闭连接
        tcpClient_.reset();
    }
    if (loopThread_) {
        loopThread_->stop();
        loopThread_.reset();
    }
    connected_ = false;
    loop_ = nullptr;
}

// ============================================================================
// 异步调用（带超时版本）
// ============================================================================

RpcAsyncClient::ResponseFuture RpcAsyncClient::asyncCall(
    const std::string& service_name,
    const std::string& method_name,
    const RpcRequest& request,
    int timeout_ms) {
    
    // FIX: 非法值用默认值兜底，防止 future 永久阻塞
    if (timeout_ms <= 0) timeout_ms = 5000;
    
    uint64_t reqId = nextReqId_.fetch_add(1);

    ResponsePromise promise;
    ResponseFuture future = promise.get_future();

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingPromises_[reqId] = std::move(promise);
    }

    // FIX: 注册超时定时器
    if (timeout_ms > 0 && loop_) {
        loop_->runAfter(timeout_ms / 1000.0, [this, reqId]() {
            handleTimeout(reqId);
        });
    }

    std::string packet = Codec::encodeRequest(request, reqId, service_name, method_name);
    // Issue #9 fix: 序列化失败时立即通知调用者
    if (packet.empty()) {
        handleTimeout(reqId);
    } else {
        sendRequest(reqId, packet);
    }

    return future;
}

void RpcAsyncClient::asyncCall(
    const std::string& service_name,
    const std::string& method_name,
    const RpcRequest& request,
    ResponseCallback cb,
    int timeout_ms) {

    // FIX: 非法值用默认值兜底
    if (timeout_ms <= 0) timeout_ms = 5000;

    uint64_t reqId = nextReqId_.fetch_add(1);

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingCallbacks_[reqId] = std::move(cb);
    }

    // FIX: 注册超时定时器
    if (timeout_ms > 0 && loop_) {
        loop_->runAfter(timeout_ms / 1000.0, [this, reqId]() {
            handleTimeout(reqId);
        });
    }

    std::string packet = Codec::encodeRequest(request, reqId, service_name, method_name);
    // Issue #9 fix: 序列化失败时立即通知调用者
    if (packet.empty()) {
        handleTimeout(reqId);
    } else {
        sendRequest(reqId, packet);
    }
}

void RpcAsyncClient::sendRequest(uint64_t req_id, const std::string& packet) {
    // Issue #8 fix: 先捕获 shared_ptr 副本，消除 TOCTOU 数据竞争
    auto client = tcpClient_;
    if (disconnecting_.load() || !loop_ || !client) {
        handleTimeout(req_id);
        return;
    }
    auto conn = client ? client->connection() : nullptr;
    if (!conn || disconnecting_.load()) {
        handleTimeout(req_id);
        return;
    }

    // FIX: 用 shared_ptr 避免跨线程拷贝
    auto packetPtr = std::make_shared<std::string>(packet);

    if (loop_->isInLoopThread()) {
         // 重新获取一次，确保最新状态
        conn = client->connection();
        if (conn) {
            conn->send(*packetPtr);
        }else {
            // FIX: 同线程下连接也可能刚断开
            handleTimeout(req_id);
        }
    } else {
        // FIX: 捕获 req_id 和 this，投递后连接断开可立即失败
        loop_->runInLoop([this, req_id, packetPtr, client]() {
            auto conn = client->connection();
            if (conn) {
                conn->send(*packetPtr);
            } else {
                handleTimeout(req_id);  // 立即失败，不等超时定时器
            }
        });
    }
}

// ============================================================================
// 超时处理
// ============================================================================

void RpcAsyncClient::handleTimeout(uint64_t req_id) {
    std::lock_guard<std::mutex> lock(pendingMutex_);

    auto promiseIt = pendingPromises_.find(req_id);
    if (promiseIt != pendingPromises_.end()) {
        RpcResponse errorResp;
        errorResp.set_success(false);
        errorResp.set_error_msg("request timeout");

        auto promise = std::move(promiseIt->second);
        pendingPromises_.erase(promiseIt);

        try {
            promise.set_value(errorResp);
        } catch (...) {}
        return;
    }

    auto callbackIt = pendingCallbacks_.find(req_id);
    if (callbackIt != pendingCallbacks_.end()) {
        RpcResponse errorResp;
        errorResp.set_success(false);
        errorResp.set_error_msg("request timeout");

        auto cb = std::move(callbackIt->second);
        pendingCallbacks_.erase(callbackIt);

        try {
            cb(errorResp);
        } catch (...) {}
    }
}

// ============================================================================
// 消息处理
// ============================================================================

void RpcAsyncClient::onConnection(const TcpConnectionPtr& conn) {
}

void RpcAsyncClient::onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    DecodedPacket decoded;
    while (Codec::decode(*buf, decoded)) {
        if (decoded.msg_type == MsgType::RESPONSE) {
            handleResponse(decoded);
        }
    }
}

void RpcAsyncClient::handleResponse(const DecodedPacket& packet) {
    uint64_t reqId = packet.req_id;

    std::lock_guard<std::mutex> lock(pendingMutex_);

    // FIX: 先 erase 再 set_value，异常安全
    auto promiseIt = pendingPromises_.find(reqId);
    if (promiseIt != pendingPromises_.end()) {
        auto promise = std::move(promiseIt->second);
        pendingPromises_.erase(promiseIt);

        try {
            promise.set_value(packet.rpc_response);
        } catch (...) {
            // promise 可能已经被设置过（比如超时先触发了）
        }
        return;
    }

    auto callbackIt = pendingCallbacks_.find(reqId);
    if (callbackIt != pendingCallbacks_.end()) {
        auto cb = std::move(callbackIt->second);
        pendingCallbacks_.erase(callbackIt);

        try {
            cb(packet.rpc_response);
        } catch (...) {
            // 回调异常，忽略
        }
    }
}

void RpcAsyncClient::onWriteComplete(const TcpConnectionPtr& conn) {
}

// ============================================================================
// 心跳
// ============================================================================

bool RpcAsyncClient::connected() const {
    return connected_;
}

void RpcAsyncClient::startHeartbeat(double intervalSeconds) {
    if (loop_ && !heartbeatRunning_.exchange(true)) {
        auto client = tcpClient_;  // shared_ptr 副本
        heartbeatTimerId_ = loop_->runEvery(intervalSeconds, [this, client]() {
            auto conn = client ? client->connection() : nullptr;
            if (connected_ && conn) {
                rpc::Heartbeat hb;
                hb.set_service_name("client");
                hb.set_node_id("node_1");
                hb.set_timestamp(std::time(nullptr));

                std::string packet = Codec::encodeHeartbeat(hb, 0);
                conn->send(packet);
            }
        });
    }
}

void RpcAsyncClient::stopHeartbeat() {
    if (heartbeatRunning_.exchange(false) && loop_) {
        loop_->cancelTimer(heartbeatTimerId_);
    }
}

} // namespace rpc