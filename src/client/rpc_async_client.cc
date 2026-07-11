// src/client/rpc_async_client.cc
#include "client/rpc_async_client.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

namespace rpc {

RpcAsyncClient::RpcAsyncClient(const std::string& host, uint16_t port)
    : host_(host), port_(port), loop_(nullptr), connected_(false), nextReqId_(1) {
}

RpcAsyncClient::~RpcAsyncClient() {
    disconnect();
}

bool RpcAsyncClient::connect() {
    loopThread_ = std::make_unique<EventLoopThread>();
    loop_ = loopThread_->startLoop();

    std::promise<bool> connectPromise;
    auto connectFuture = connectPromise.get_future();

    loop_->runInLoop([this, &connectPromise]() {
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

        int ret = ::connect(sockfd, reinterpret_cast<struct sockaddr*>(&serverAddr), sizeof(serverAddr));
        if (ret < 0 && errno != EINPROGRESS) {
            ::close(sockfd);
            connectPromise.set_value(false);
            return;
        }

        struct sockaddr_in localAddr;
        memset(&localAddr, 0, sizeof(localAddr));
        socklen_t addrlen = sizeof(localAddr);
        getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&localAddr), &addrlen);

        connection_.reset(new TcpConnection(loop_, "client", sockfd, localAddr, serverAddr));
        connection_->setConnectionCallback(
            [this, &connectPromise](const TcpConnectionPtr& conn) {
                if (conn->getContext() == nullptr) {
                    connected_ = true;
                    conn->setContext(reinterpret_cast<void*>(1));
                    connectPromise.set_value(true);
                }
            });
        connection_->setMessageCallback(
            std::bind(&RpcAsyncClient::onMessage, this, 
                      std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        connection_->setWriteCompleteCallback(
            std::bind(&RpcAsyncClient::onWriteComplete, this, std::placeholders::_1));
        connection_->setCloseCallback(
            [this](const TcpConnectionPtr& conn) {
                connected_ = false;
            });

        connection_->connectEstablished();
    });

    return connectFuture.get();
}

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

    // 编码：直接得到完整包（header + body + checksum），无需再加长度头
    std::string packet = Codec::encodeRequest(request, reqId, service_name, method_name);

    sendRequest(reqId, packet);

    return future;
}

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

    sendRequest(reqId, packet);
}

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
}

// ============================================================================
// onMessage - 消息到达回调（IO 线程）
// ============================================================================
// 新协议：直接从 Buffer 中解码完整包（header + body + checksum）
// 无需先 decodeWithLength 再 decode，Codec::decode 一次完成
// ============================================================================
void RpcAsyncClient::onMessage(const TcpConnectionPtr& conn, Buffer* buf, int64_t) {
    DecodedPacket decoded;
    // 循环解码：一次可读事件可能收到多个完整包
    while (Codec::decode(*buf, decoded)) {
        if (decoded.msg_type == MsgType::RESPONSE) {
            handleResponse(decoded);
        }
        // TODO: 处理 HEARTBEAT_ACK 等消息类型
    }
}

void RpcAsyncClient::handleResponse(const DecodedPacket& packet) {
    uint64_t reqId = packet.req_id;

    {
        std::lock_guard<std::mutex> lock(pendingMutex_);

        auto promiseIt = pendingPromises_.find(reqId);
        if (promiseIt != pendingPromises_.end()) {
            promiseIt->second.set_value(packet.rpc_response);
            pendingPromises_.erase(promiseIt);
            return;
        }

        auto callbackIt = pendingCallbacks_.find(reqId);
        if (callbackIt != pendingCallbacks_.end()) {
            callbackIt->second(packet.rpc_response);
            pendingCallbacks_.erase(callbackIt);
        }
    }
}

void RpcAsyncClient::onWriteComplete(const TcpConnectionPtr& conn) {
}

bool RpcAsyncClient::connected() const {
    return connected_;
}

void RpcAsyncClient::startHeartbeat(double intervalSeconds) {
    if (loop_ && !heartbeatRunning_.exchange(true)) {
        loop_->runInLoop([this, intervalSeconds]() {
            loop_->runEvery(intervalSeconds, [this]() {
                if (connected_ && connection_) {
                    rpc::Heartbeat hb;
                    hb.set_service_name("client");
                    hb.set_node_id("node_1");
                    hb.set_timestamp(std::time(nullptr));

                    // 心跳包同样直接编码为完整包，无需加长度头
                    std::string packet = Codec::encodeHeartbeat(hb, 0);
                    connection_->send(packet);
                }
            });
        });
    }
}

void RpcAsyncClient::stopHeartbeat() {
    heartbeatRunning_ = false;
}

} // namespace rpc