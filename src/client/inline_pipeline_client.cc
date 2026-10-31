// src/client/inline_pipeline_client.cc
#include "client/inline_pipeline_client.h"

#include <arpa/inet.h>
#include <cstring>
#include <utility>
#include <vector>

namespace rpc {

namespace {
using Clock = std::chrono::steady_clock;

int msUntil(Clock::time_point deadline) {
    auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
                      deadline - Clock::now()).count();
    if (remain <= 0) return 0;
    if (remain > 3600 * 1000) remain = 3600 * 1000;
    return static_cast<int>(remain);
}

double usBetween(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::micro>(b - a).count();
}

RpcResponse makeTimeoutResponse() {
    RpcResponse resp;
    resp.set_success(false);
    resp.set_error_msg("request timeout");
    return resp;
}
}  // namespace

InlinePipelineClient::InlinePipelineClient(const std::string& host, uint16_t port) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        return;
    }

    client_ = std::make_shared<TcpClient>(&loop_, addr);
    client_->setConnectionCallback(
        [this](const TcpConnectionPtr& conn) { onConnection(conn); });
    client_->setMessageCallback(
        [this](const TcpConnectionPtr& conn, Buffer* buf, int64_t t) {
            onMessage(conn, buf, t);
        });
    valid_ = true;
}

InlinePipelineClient::~InlinePipelineClient() {
    // client_ 声明在 loop_ 之后 → 先析构 client_，此时 loop_ 仍存活
    client_.reset();
}

bool InlinePipelineClient::connect(int timeoutMs) {
    if (!valid_ || !client_) return false;

    client_->connect();

    const auto deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
    while (true) {
        auto conn = client_->connection();
        if (conn && conn->connected()) return true;

        int remain = msUntil(deadline);
        if (remain <= 0) return false;
        loop_.loopOnce(remain);
    }
}

bool InlinePipelineClient::connected() const {
    if (!client_) return false;
    auto conn = client_->connection();
    return conn && conn->connected();
}

void InlinePipelineClient::onConnection(const TcpConnectionPtr&) {
    // 连接状态通过 client_->connection() 查询，这里不需要额外处理
}

void InlinePipelineClient::onMessage(const TcpConnectionPtr&, Buffer* buf, int64_t) {
    DecodedPacket packet;
    while (Codec::decode(*buf, packet)) {
        if (packet.msg_type != MsgType::RESPONSE) continue;

        auto it = pending_.find(packet.req_id);
        if (it == pending_.end()) continue;  // 已超时或未知请求

        Pending p = std::move(it->second);
        pending_.erase(it);

        const double us = usBetween(p.sentAt, Clock::now());
        if (p.cb) p.cb(packet.rpc_response, us);
        ++completions_;
    }
}

bool InlinePipelineClient::submit(const std::string& service,
                                  const std::string& method,
                                  const RpcRequest& request,
                                  int timeoutMs,
                                  CompletionCallback cb) {
    auto conn = client_ ? client_->connection() : nullptr;
    if (!conn || !conn->connected()) return false;

    const uint64_t id = nextId_++;

    std::string packet = Codec::encodeRequest(request, id, service, method);
    if (packet.empty()) return false;

    Pending p;
    p.id = id;
    p.sentAt = Clock::now();
    p.deadline = p.sentAt + std::chrono::milliseconds(timeoutMs > 0 ? timeoutMs : 3000);
    p.cb = std::move(cb);
    pending_.emplace(id, std::move(p));

    // 当前线程就是 loop 线程 → 直接 write()，不经过 eventfd
    conn->send(packet);
    return true;
}

int InlinePipelineClient::nextWaitMs() const {
    if (pending_.empty()) return 1000;

    auto earliest = Clock::time_point::max();
    for (const auto& kv : pending_) {
        if (kv.second.deadline < earliest) earliest = kv.second.deadline;
    }
    return msUntil(earliest);
}

int InlinePipelineClient::sweepTimeouts() {
    if (pending_.empty()) return 0;

    const auto now = Clock::now();
    std::vector<std::pair<uint64_t, Pending>> expired;

    for (auto it = pending_.begin(); it != pending_.end();) {
        if (it->second.deadline <= now) {
            expired.emplace_back(it->first, std::move(it->second));
            it = pending_.erase(it);
        } else {
            ++it;
        }
    }

    const RpcResponse err = makeTimeoutResponse();
    for (auto& kv : expired) {
        const double us = usBetween(kv.second.sentAt, now);
        if (kv.second.cb) kv.second.cb(err, us);
        ++completions_;
    }
    return static_cast<int>(expired.size());
}

int InlinePipelineClient::pumpOnce() {
    completions_ = 0;

    // epoll_wait 的超时 = 最近一个请求的剩余时间（没有在途时给个默认值）
    loop_.loopOnce(nextWaitMs());

    // 一轮事件处理完，再统一处理到期请求（避免每个请求一个定时器）
    sweepTimeouts();

    const int done = completions_;
    completions_ = 0;
    return done;
}

}  // namespace rpc
