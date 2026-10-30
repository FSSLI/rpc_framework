// src/client/inline_sync_client.cc
#include "client/inline_sync_client.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstring>

namespace rpc {

namespace {
using Clock = std::chrono::steady_clock;

int msUntil(Clock::time_point deadline) {
    auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
    if (remain <= 0) return 0;
    if (remain > 3600 * 1000) remain = 3600 * 1000;
    return static_cast<int>(remain);
}
}  // namespace

InlineSyncClient::InlineSyncClient(const std::string& host, uint16_t port) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        return;
    }

    client_ = std::make_shared<TcpClient>(&loop_, addr);
    client_->setConnectionCallback([this](const TcpConnectionPtr& conn) {
        onConnection(conn);
    });
    client_->setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, int64_t t) {
            onMessage(conn, buf, t);
        });
    valid_ = true;
}

InlineSyncClient::~InlineSyncClient() {
    // client_ 声明在 loop_ 之后 → 先析构 client_（仍在本线程，Connector 的 assertInLoopThread 成立）
    client_.reset();
}

bool InlineSyncClient::connect(int timeoutMs) {
    if (!valid_ || !client_) return false;

    connectDone_ = false;
    connectOk_ = false;
    client_->connect();

    const auto deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
    while (!connectDone_) {
        int remain = msUntil(deadline);
        if (remain <= 0) break;
        loop_.loopOnce(remain);
    }
    return connectOk_;
}

bool InlineSyncClient::connected() const {
    if (!client_) return false;
    auto conn = client_->connection();
    return conn && conn->connected();
}

void InlineSyncClient::onConnection(const TcpConnectionPtr& conn) {
    connectDone_ = true;
    connectOk_ = conn->connected();

    // 调用过程中连接断开：立即结束等待，避免白等到超时
    if (!connectOk_ && pendingId_ != 0 && !done_) {
        result_.Clear();
        result_.set_success(false);
        result_.set_error_msg("connection closed");
        done_ = true;
    }
}

void InlineSyncClient::onMessage(const TcpConnectionPtr&, Buffer* buf, int64_t) {
    DecodedPacket packet;
    while (Codec::decode(*buf, packet)) {
        if (packet.msg_type != MsgType::RESPONSE) continue;
        if (packet.req_id != pendingId_ || done_) continue;
        result_.CopyFrom(packet.rpc_response);
        done_ = true;
    }
}

RpcResponse InlineSyncClient::call(const std::string& service,
                                   const std::string& method,
                                   const RpcRequest& request,
                                   int timeoutMs) {
    RpcResponse errorResp;
    errorResp.set_success(false);

    auto conn = client_ ? client_->connection() : nullptr;
    if (!conn || !conn->connected()) {
        errorResp.set_error_msg("not connected");
        return errorResp;
    }

    pendingId_ = nextId_++;
    done_ = false;
    result_.Clear();

    std::string packet = Codec::encodeRequest(request, pendingId_, service, method);
    if (packet.empty()) {
        errorResp.set_error_msg("encode failed");
        pendingId_ = 0;
        return errorResp;
    }

    // 当前线程就是 loop 线程 → 直接 write()，不经过 eventfd 唤醒
    conn->send(packet);

    const auto deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
    while (!done_) {
        int remain = msUntil(deadline);
        if (remain <= 0) {
            errorResp.set_error_msg("request timeout");
            pendingId_ = 0;
            return errorResp;
        }
        // 在本线程里跑一轮事件循环：readv + 解码 + 回调，全部同步完成
        loop_.loopOnce(remain);
    }

    pendingId_ = 0;
    RpcResponse out;
    out.Swap(&result_);
    return out;
}

}  // namespace rpc
