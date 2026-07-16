// src/client/rpc_async_client.cc
#include "client/rpc_async_client.h"
#include "discovery/service_registry.h"
#include "circuit_breaker/circuit_breaker.h"
#include "rate_limiter/token_bucket.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <random>
#include <algorithm>  // std::shuffle
#include <thread>     // std::hash<std::thread::id>

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

RpcAsyncClient::RpcAsyncClient(std::shared_ptr<TcpClient> tcpClient)
    : host_(), port_(0),
      loop_(tcpClient ? tcpClient->getLoop() : nullptr),
      ownsLoopThread_(false),
      tcpClient_(std::move(tcpClient)),
      connected_(false),
      nextReqId_(1) {
    if (tcpClient_) {
        setupCallbacks();
        // 检查连接是否已建立
        auto conn = tcpClient_->connection();
        connected_ = (conn != nullptr && conn->connected());
    }
}

RpcAsyncClient::~RpcAsyncClient() {
    // Issue #4 fix: 先设析构标志，防止外部 loop 的 pending 定时器回调 UAF
    destroyed_ = true;
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
// 公共回调设置（直接模式 + 池模式共用）
// ============================================================================

void RpcAsyncClient::setupCallbacks() {
    if (!tcpClient_) return;

    // 设置 TcpClient 级别回调（新连接自动继承）
    auto connCb = [this](const TcpConnectionPtr& conn) {
        connected_ = conn->connected();
    };
    tcpClient_->setConnectionCallback(connCb);

    auto msgCb = std::bind(&RpcAsyncClient::onMessage, this,
                           std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    tcpClient_->setMessageCallback(msgCb);

    auto writeCb = std::bind(&RpcAsyncClient::onWriteComplete, this, std::placeholders::_1);
    tcpClient_->setWriteCompleteCallback(writeCb);

    // 如果已有活跃连接，也直接设置回调（池模式下连接已建立）
    auto conn = tcpClient_->connection();
    if (conn) {
        conn->setConnectionCallback(connCb);
        conn->setMessageCallback(msgCb);
        conn->setWriteCompleteCallback(writeCb);
    }
}

// ============================================================================
// 连接
// ============================================================================

bool RpcAsyncClient::connect() {
    // 池模式：TcpClient 已由池预建，直接返回连接状态
    if (!ownsLoopThread_) {
        return connected_.load();
    }
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

    // 连接所有节点（轮询负载均衡需要全部节点可用）
    int connected = 0;
    for (const auto& node : nodes) {
        std::cout << "RpcAsyncClient: connecting to " << serviceName_
                  << " -> " << node.host << ":" << node.port << std::endl;

        Endpoint ep;
        ep.host = node.host;
        ep.port = node.port;
        ep.loopThread = std::make_unique<EventLoopThread>();
        EventLoop* epLoop = ep.loopThread->startLoop();

        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(ep.port);
        if (inet_pton(AF_INET, ep.host.c_str(), &serverAddr.sin_addr) <= 0) {
            std::cerr << "RpcAsyncClient: invalid IP " << ep.host << std::endl;
            ep.loopThread->stop();
            continue;
        }

        ep.tcpClient = std::make_shared<TcpClient>(epLoop, serverAddr);
        ep.tcpClient->setRetryOnDisconnect(true);

        // 等待连接
        auto promisePtr = std::make_shared<std::promise<bool>>();
        auto promiseSetPtr = std::make_shared<std::atomic<bool>>(false);
        auto future = promisePtr->get_future();

        ep.tcpClient->setConnectionCallback(
            [promisePtr, promiseSetPtr](const TcpConnectionPtr& conn) {
                bool expected = false;
                if (promiseSetPtr->compare_exchange_strong(expected, true)) {
                    promisePtr->set_value(conn->connected());
                }
            });

        ep.tcpClient->connect();

        auto status = future.wait_for(std::chrono::milliseconds(3000));
        if (status != std::future_status::timeout && future.get()) {
            // 连接成功，设置消息回调
            auto msgCb = std::bind(&RpcAsyncClient::onMessage, this,
                                   std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
            ep.tcpClient->setMessageCallback(msgCb);
            // 也设置到已有连接上
            auto conn = ep.tcpClient->connection();
            if (conn) {
                conn->setMessageCallback(msgCb);
            }
            // 连接回调：更新全局连接状态
            ep.tcpClient->setConnectionCallback(
                [this](const TcpConnectionPtr& c) { connected_ = c->connected(); });

            endpoints_.push_back(std::move(ep));
            ++connected;
            if (lbPolicy_ == LBPolicy::CONSISTENT_HASH && consistentHash_) {
                consistentHash_->addNode(node.host + ":" + std::to_string(node.port));
            }
            std::cout << "RpcAsyncClient: connected to " << node.host << ":" << node.port << std::endl;
        } else {
            // 超时或失败，跳过此节点
            ep.tcpClient->stop();
            ep.loopThread->stop();
            std::cerr << "RpcAsyncClient: failed to connect to " << node.host << ":" << node.port << std::endl;
        }
    }

    if (connected == 0) {
        std::cerr << "RpcAsyncClient: all " << nodes.size() << " nodes unreachable" << std::endl;
        return false;
    }

    connected_ = true;
    std::cout << "RpcAsyncClient: " << connected << "/" << nodes.size()
              << " nodes connected for " << serviceName_ << std::endl;
    return true;
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

    // 先用基回调设置连接状态
    setupCallbacks();

    // 再覆盖连接回调加入 promise 通知（仅用于首次等待连接结果）
    auto baseConnCb = [this](const TcpConnectionPtr& conn) {
        connected_ = conn->connected();
    };
    tcpClient_->setConnectionCallback(
        [baseConnCb, promisePtr, promiseSetPtr](const TcpConnectionPtr& conn) {
            baseConnCb(conn);
            bool expected = false;
            if (promiseSetPtr->compare_exchange_strong(expected, true)) {
                promisePtr->set_value(conn->connected());
            }
        });

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
    // 连接成功后恢复纯净回调，释放 promise 相关 shared_ptr
    setupCallbacks();
    return true;
}

void RpcAsyncClient::disconnect() {
    disconnecting_ = true;

    // 清理多节点：先 reset TcpClient（loop 存活时 ~TcpClient 才安全）
    // 再 stop EventLoopThread
    std::vector<std::unique_ptr<EventLoopThread>> savedLoops;
    for (auto& ep : endpoints_) {
        if (lbPolicy_ == LBPolicy::CONSISTENT_HASH && consistentHash_) {
            consistentHash_->removeNode(ep.host + ":" + std::to_string(ep.port));
        }
        if (ep.tcpClient) {
            ep.tcpClient->setConnectionCallback({});
            ep.tcpClient->setMessageCallback({});
            ep.tcpClient->stop();
            ep.tcpClient.reset();
        }
        savedLoops.push_back(std::move(ep.loopThread));
    }
    endpoints_.clear();
    for (auto& lt : savedLoops) {
        if (lt) lt->stop();
    }

    // 清理单节点 — 先清除回调防止 pool 模式下 TcpConnection 回调 UAF
    if (tcpClient_) {
        tcpClient_->setConnectionCallback({});
        tcpClient_->setMessageCallback({});
        tcpClient_->setWriteCompleteCallback({});
        // 同样清除已活跃连接上的回调（setupCallbacks 会同时设 TcpClient 和 TcpConnection）
        auto conn = tcpClient_->connection();
        if (conn) {
            conn->setConnectionCallback({});
            conn->setMessageCallback({});
            conn->setWriteCompleteCallback({});
        }
        tcpClient_->disconnectPermanently();
        tcpClient_.reset();
    }
    if (ownsLoopThread_ && loopThread_) {
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

    // 熔断检查
    if (circuitBreaker_ && !circuitBreaker_->allowRequest()) {
        RpcResponse errorResp;
        errorResp.set_success(false);
        errorResp.set_error_msg("circuit breaker open");
        ResponsePromise promise;
        promise.set_value(errorResp);
        return promise.get_future();
    }

    // 限流检查
    if (rateLimiter_ && !rateLimiter_->allow()) {
        RpcResponse errorResp;
        errorResp.set_success(false);
        errorResp.set_error_msg("rate limit exceeded");
        ResponsePromise promise;
        promise.set_value(errorResp);
        return promise.get_future();
    }

    uint64_t reqId = nextReqId_.fetch_add(1);

    // TraceID 注入：reqId + 时间戳 + 线程 ID
    RpcRequest tracedReq = request;
    std::string traceId = std::to_string(reqId) + "-"
                        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-"
                        + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    (*tracedReq.mutable_metadata())["trace-id"] = traceId;

    ResponsePromise promise;
    ResponseFuture future = promise.get_future();

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingPromises_[reqId] = std::move(promise);
    }

    // 注册超时定时器（多节点模式用首节点 loop，单节点用 loop_）
    EventLoop* timeoutLoop = loop_;
    if (!endpoints_.empty() && !endpoints_[0].tcpClient) {
    } else if (!endpoints_.empty()) {
        timeoutLoop = endpoints_[0].tcpClient->getLoop();
    }
    if (timeout_ms > 0 && timeoutLoop) {
        timeoutLoop->runAfter(timeout_ms / 1000.0, [this, reqId]() {
            handleTimeout(reqId);
        });
    }

    std::string packet = Codec::encodeRequest(tracedReq, reqId, service_name, method_name);
    if (packet.empty()) {
        handleTimeout(reqId);
    } else {
        sendRequest(reqId, packet, service_name + "/" + method_name);
    }

    return future;
}

void RpcAsyncClient::asyncCall(
    const std::string& service_name,
    const std::string& method_name,
    const RpcRequest& request,
    ResponseCallback cb,
    int timeout_ms) {

    // 熔断检查
    if (circuitBreaker_ && !circuitBreaker_->allowRequest()) {
        RpcResponse errorResp;
        errorResp.set_success(false);
        errorResp.set_error_msg("circuit breaker open");
        cb(errorResp);
        return;
    }

    // 限流检查
    if (rateLimiter_ && !rateLimiter_->allow()) {
        RpcResponse errorResp;
        errorResp.set_success(false);
        errorResp.set_error_msg("rate limit exceeded");
        cb(errorResp);
        return;
    }

    uint64_t reqId = nextReqId_.fetch_add(1);

    RpcRequest tracedReq = request;
    std::string traceId = std::to_string(reqId) + "-"
                        + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-"
                        + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    (*tracedReq.mutable_metadata())["trace-id"] = traceId;

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingCallbacks_[reqId] = std::move(cb);
    }

    EventLoop* timeoutLoop = loop_;
    if (!endpoints_.empty() && endpoints_[0].tcpClient) {
        timeoutLoop = endpoints_[0].tcpClient->getLoop();
    }
    if (timeout_ms > 0 && timeoutLoop) {
        timeoutLoop->runAfter(timeout_ms / 1000.0, [this, reqId]() {
            handleTimeout(reqId);
        });
    }

    std::string packet = Codec::encodeRequest(tracedReq, reqId, service_name, method_name);
    if (packet.empty()) {
        handleTimeout(reqId);
    } else {
        sendRequest(reqId, packet, service_name + "/" + method_name);
    }
}

void RpcAsyncClient::setLBPolicy(LBPolicy p, int virtualNodes) {
    lbPolicy_ = p;
    if (p == LBPolicy::CONSISTENT_HASH) {
        consistentHash_ = std::make_unique<ConsistentHash>(virtualNodes);
        for (auto& ep : endpoints_) {
            consistentHash_->addNode(ep.host + ":" + std::to_string(ep.port));
        }
    } else {
        consistentHash_.reset();
    }
}

std::shared_ptr<TcpClient> RpcAsyncClient::getTcpClient(const std::string& hashKey) {
    size_t sz = endpoints_.size();
    if (sz == 0) return tcpClient_;

    if (lbPolicy_ == LBPolicy::CONSISTENT_HASH && consistentHash_ && !hashKey.empty()) {
        std::string node = consistentHash_->getNode(hashKey);
        for (auto& ep : endpoints_) {
            if (ep.host + ":" + std::to_string(ep.port) == node) {
                return ep.tcpClient;
            }
        }
        // 哈希环未命中，fallback 轮询
    }
    // 默认轮询
    size_t idx = rrIndex_.fetch_add(1, std::memory_order_relaxed) % sz;
    return endpoints_[idx].tcpClient;
}

void RpcAsyncClient::sendRequest(uint64_t req_id, const std::string& packet, const std::string& hashKey) {
    auto client = getTcpClient(hashKey);
    if (disconnecting_.load() || !client) {
        handleTimeout(req_id);
        return;
    }
    auto conn = client->connection();
    if (!conn) {
        handleTimeout(req_id);
        return;
    }
    conn->send(packet);
}

// ============================================================================
// 超时处理
// ============================================================================

void RpcAsyncClient::handleTimeout(uint64_t req_id) {
    // Issue #4 fix: 对象已析构则直接返回，防止访问已销毁成员
    if (destroyed_.load()) return;

    // 超时 = 失败，通知熔断器
    if (circuitBreaker_) {
        circuitBreaker_->recordFailure();
    }

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

    // Issue #7 fix: 熔断只计网络/系统级失败，业务 error 不触发
    if (circuitBreaker_) {
        if (packet.network_status == Status::SUCCESS) {
            circuitBreaker_->recordSuccess();
        } else {
            circuitBreaker_->recordFailure();
        }
    }

    std::lock_guard<std::mutex> lock(pendingMutex_);

    auto promiseIt = pendingPromises_.find(reqId);
    if (promiseIt != pendingPromises_.end()) {
        auto promise = std::move(promiseIt->second);
        pendingPromises_.erase(promiseIt);

        try {
            promise.set_value(packet.rpc_response);
        } catch (...) {}
        return;
    }

    auto callbackIt = pendingCallbacks_.find(reqId);
    if (callbackIt != pendingCallbacks_.end()) {
        auto cb = std::move(callbackIt->second);
        pendingCallbacks_.erase(callbackIt);

        try {
            cb(packet.rpc_response);
        } catch (...) {}
    }
}

void RpcAsyncClient::onWriteComplete(const TcpConnectionPtr& conn) {
}

// ============================================================================
// 心跳
// ============================================================================

bool RpcAsyncClient::connected() const {
    if (!endpoints_.empty()) {
        // 多节点模式：任一节点连接即视为就绪
        for (auto& ep : endpoints_) {
            auto conn = ep.tcpClient ? ep.tcpClient->connection() : nullptr;
            if (conn && conn->connected()) return true;
        }
        return false;
    }
    return connected_;
}

void RpcAsyncClient::startHeartbeat(double intervalSeconds) {
    // 确定心跳注册的目标 EventLoop
    EventLoop* hbLoop = loop_;
    if (!endpoints_.empty() && endpoints_[0].tcpClient) {
        hbLoop = endpoints_[0].tcpClient->getLoop();
    }
    if (hbLoop && !heartbeatRunning_.exchange(true)) {
        heartbeatLoop_ = hbLoop;  // track which loop the timer is on
        auto client = tcpClient_;
        heartbeatTimerId_ = hbLoop->runEvery(intervalSeconds, [this, client]() {
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
    if (heartbeatRunning_.exchange(false) && heartbeatLoop_) {
        heartbeatLoop_->cancelTimer(heartbeatTimerId_);
        heartbeatLoop_ = nullptr;
    }
}

} // namespace rpc