# RPC 框架项目：问题、分析、优化与复盘

> 文档性质：项目复盘 + 学习笔记（写给自己，也可用于面试准备）
> 时间范围：2026.02 项目立项 → 2026.07 简历投递 → 2026.10 系统性复盘与优化
> 对应代码：`FSSLI/rpc_framework` @ `ed9f70e`
> 环境：WSL2 / Ubuntu 22.04 / i7-13620H（8 核 16 线程）/ 16 vCPU / 7 GB / g++ 11.4 / protobuf 3.12
> 数据来源：`docs/bench_results_wsl.md`（所有数字可用附录命令复现）

---

## 目录

1. [一页纸总览](#1-一页纸总览)
2. [第一部分：原来的问题](#2-第一部分原来的问题)
3. [第二部分：怎么分析和定位](#3-第二部分怎么分析和定位)
4. [第三部分：具体改了什么](#4-第三部分具体改了什么)
5. [第四部分：新项目的数据表现](#5-第四部分新项目的数据表现)
6. [第五部分：值得讲的技术点](#6-第五部分值得讲的技术点)
7. [第六部分：还有什么不足](#7-第六部分还有什么不足)
8. [第七部分：方法论复盘](#8-第七部分方法论复盘)
9. [附录：命令、数字、文件索引](#9-附录命令数字文件索引)

---

## 1. 一页纸总览

### 1.1 一句话

这个项目不是"功能做完了就完了"，而是经历了一次**从"我觉得快"到"我知道快到哪、慢在哪"**的转变：先用正确的测量方法找到真实上限，再用系统调用账单找到真实瓶颈，最后用一次重构拿到 2.14 倍提升，并顺手挖出三个内存安全 bug。

### 1.2 关键数字对比

| 指标 | 简历版（7 月） | 复盘后（10 月） | 变化 |
|---|---|---|---|
| 吞吐 | 48,765 QPS | 闭环 61,683 / **流水线 101 万** | 见下文口径说明 |
| p99 延迟 | 382 µs | 闭环 461 µs（流水线 2.2 ms） | 口径不同 |
| 系统调用 / 请求 | 未知（没测过） | **14.15 → 6.06** | −57% |
| 每请求 CPU | 未知 | 93.6 µs → **46.7 µs** | −50% |
| 端到端 RTT | 165 µs（反解） | 129.7 µs（闭环）/ 61.9 µs（inline） | −22% / −62% |
| 测试 | 12 项 | 13 项 + ASan | +1 项 + 内存校验 |
| 已知内存 bug | 未知 | 发现并修复 3 个 UAF | — |

### 1.3 最重要的认知转变

> **48,765 QPS 不是吞吐上限，而是"在途请求数 = 线程数"这个测量方式的上限。**
>
> `QPS = 在途请求数 / RTT`。压测时每个线程发一个请求、`future.get()` 等回来再发下一个，在途数恒等于 8（线程数），所以 8 / 165 µs ≈ 48,765。把在途数提上去，同一台机器能跑 101 万。

---

## 2. 第一部分：原来的问题

问题分三层：**表述层**（简历怎么写）、**实现层**（代码怎么跑）、**认知层**（为什么会犯这些错）。

### 2.1 表述层：简历里的五个问题

| # | 原表述 | 事实 | 风险等级 |
|---|---|---|---|
| 1 | 「对标 **gRPC** 架构」 | gRPC 的核心是 HTTP/2 多路复用 + 四种 streaming；本项目是自定义二进制协议、只有 unary | 🔴 高：一句"你的 HTTP/2 帧在哪"就问穿 |
| 2 | 「Wireshark 验证**跨语言解码一致**」 | 仓库里没有任何第二语言的解码器，只有 C++ | 🔴 高：事实不成立 |
| 3 | 「实现**主从协作**的高并发网络层」 | 结构上有主从骨架，但 `numThreads_` 默认 0，运行期全部连接回落主 Reactor | 🟡 中：说法没错，但"运行期不是" |
| 4 | 「**解决**断线恢复与重连风暴」 | 只有指数退避，**没有随机抖动**。没有 jitter 只降低单连接频率，不解决同步重连 | 🟡 中：概念上不成立 |
| 5 | 「8 线程 8 连接……吞吐 **+60%**」 | 复测 41,051 → 61,683 = **+50%**；+60% 是更早一版配置 | 🟢 低：数字口径问题 |

> 注：第 2 条最终决定不做跨语言实现，所以在简历中直接改为"Wireshark 抓包核对字段布局与字节序"。

### 2.2 实现层：代码里的真实问题

#### （1）异步 API 名不副实（架构级）

`src/client/rpc_async_client.cc:392-435`：

```cpp
RpcResponse RpcAsyncClient::doCall(...) {
    ...
    return future.get();          // ← 阻塞等待
}

ResponseFuture RpcAsyncClient::asyncCall(...) {
    RpcResponse result = doCall(...);   // ← 返回前就等完了
    ResponsePromise promise;
    promise.set_value(result);
    return promise.get_future();        // ← 拿到的 future 已经 ready
}
```

**后果**：Future 版 `asyncCall` 是同步的，每个线程同时只能有 1 个在途请求。
只有**回调版**（`:437-478`，且不配拦截器）是真异步；一旦配置了拦截器，`chain_.execute` 里调的还是 `doCall`，又退化成阻塞。

#### （2）服务端默认是单 Reactor

- `src/network/event_loop_thread_pool.cc:12`：`numThreads_(0)`
- `:38-50`：`loops_` 为空时 `getNextLoop()` 恒返回 `baseLoop_`
- `setThreadNum()` 全项目零调用点

**后果**：Acceptor 与所有 TcpConnection 共享一个 EventLoop，accept / read / 解码 / 业务 / 编码 / write 全在一个线程。这解释了为什么服务端热路径上其实**没有**跨线程唤醒。

#### （3）每请求一个 3 秒超时定时器，且成功不取消

`src/client/rpc_async_client.cc:421-425` 每次调用都 `runAfter(3.0s)`，而 `handleResponse` 只删 promise、不 `cancelTimer`。

`runAfter` 的代价（`event_loop.cc:187-208`）：拿 `timerMutex_` → `make_shared<bool>` → `unordered_map` 插入 → 优先队列 push（O(log n)）→ 跨线程时**一次 `write(eventfd)`**。

**规模**：48k QPS × 3 s = 约 14 万个存活但无用的定时器。

#### （4）每次调用两次线程跳转

```
worker --eventfd--> IO 线程 --write--> 服务端
worker <--futex---- IO 线程 <--read--- 服务端
```

一次本机 RTT 里塞进了 2 次 eventfd 写、1 次 futex 唤醒、2 次线程切换。

#### （5）连接池把同一条连接交给两个客户端

`ConnectionPool::acquire` 轮询返回 `TcpClient`，线程数 > 连接数时会重复；而 `RpcAsyncClient` 池模式构造时 `setupCallbacks()` 会**覆盖**连接的 3 个回调。

**实测**：8 线程 / 4 连接 → **4 个错误、耗时卡在 3.00 s、QPS 16,658**（正常应为 6 万级）。

#### （6）三层 use-after-free（最严重）

`test_connection_pool` 偶发 segfault（约 1/15 次）。ASan 下 1/1 必现，而且是三个独立 bug：

| # | 崩溃点 | 触发链 |
|---|---|---|
| 1 | `Channel::handleEvent` 读 `revents_` | `handleRead` → `handleClose` → `TcpConnection`（连同 Channel）析构 → 返回后继续读成员 |
| 2 | `Channel::handleEvent` 读 `readCallback_` | `errorCallback_` → `Connector::handleError` → `removeAndResetChannel` → Channel 析构 → 返回后继续 |
| 3 | `TcpClient::removeConnection` 读成员 | 主线程析构 `TcpClient` 后，IO 线程仍通过 `closeCallback_` 的裸 `this` 回调 |

#### （7）两处"定义了但没用"的浪费

- `Socket::setTcpNoDelay()` 从未被调用 → Nagle 一直开着
- `RpcAsyncClient::setupCallbacks()` 注册了 `writeCompleteCallback`，但 `onWriteComplete()` 是空函数 → 每次 send 多一次 `std::bind` + 入队

#### （8）文档与代码不一致

- README 结构树缺新增文件
- `docs/network_layer.md:835` 写"每个连接一个线程"（概念错误）
- README 里流水线数据同时存在"110~117 万"和"101 万"两种口径

### 2.3 认知层：为什么这些问题一直没被发现

| 原因 | 说明 |
|---|---|
| **没有 baseline** | 只有一个"最好成绩"，没有对照，改了什么、有没有用都说不清 |
| **测量方法错了** | 闭环同步压测把"延迟"当成了"吞吐" |
| **没有 profiling** | 从没跑过 `strace -c` / `perf`，成本结构全靠猜 |
| **没有内存检查** | 12 项测试全绿，但常规测试抓不到窄窗口的 UAF |
| **测试只覆盖正常路径** | 没有"并发数 > 连接数""服务端先关连接"这类异常场景 |
| **文档靠记忆** | 代码改了，结构树和结论没同步 |

---

## 3. 第二部分：怎么分析和定位

### 3.1 第一步：校准测量口径（最重要的一步）

**观察**：压测代码 `tests/test_benchmark.cc:59-69` 是"发一个、等一个"。

**推导**：

```
QPS = 在途请求数 / RTT
    = 线程数 / 单次往返延迟
```

用 README 的数据反解 RTT：

| 线程 | QPS | 反解 RTT | 观察 |
|---|---|---|---|
| 1 | 11,842 | 84.4 µs | 基线 |
| 2 | 19,795 | 101.0 µs | +20% |
| 4 | 34,397 | 116.3 µs | +38% |
| 8 | 48,442 | 165.1 µs | **+96%，但吞吐只涨 4.09×** |

**结论**：扩展性衰减的形态是"延迟随并发上升"，这是共享资源排队或 CPU 超卖的典型特征，不是算法问题。

**验证**：后来实测 8/153 µs = 52.3k（实测 51.2k）、8/125 µs = 64k（实测 61.9k），误差 2~3%。

### 3.2 第二步：单变量 A/B（推翻两个假设）

**假设 A：服务端只有一个 Reactor，是主要瓶颈。**

做法：给 `RpcServer` 补 `setThreadNum` 转发（原来只有 `TcpServer` 有），benchmark 加参数，跑 1/2/4/8。

| 服务端从 Reactor | QPS | avg | p99 |
|---|---|---|---|
| 1 | 51,240 | 153 µs | 833 µs |
| 2 | 55,224 | 140 µs | 693 µs |
| 4 | 57,111 | 136 µs | 634 µs |
| 8 | **61,901** | **125 µs** | **508 µs** |

**结果：只 +21%。假设被证伪。**

**假设 B：每请求的超时定时器是主要开销。**

做法：`timeout_ms = 0` 时 `doCall` 里的 `if (timeout_ms > 0 …)` 不成立，`runAfter` 完全不执行。

| 超时 | QPS | avg | p99 |
|---|---|---|---|
| 3000 ms（每请求一个定时器） | 57,771 | 135 µs | 565 µs |
| 0（不注册定时器） | 58,594 | 133 µs | 557 µs |

**结果：只 +1.4%。假设也被证伪。**

> 这一步的价值：**先证伪，再优化**。否则会把时间花在错的地方。

### 3.3 第三步：系统调用账单（找到真因）

```
strace -c -f -e trace=write,read,readv,writev,epoll_wait,futex,eventfd2 \
  ./test_benchmark 2000 4 8 3000 only
```

8,000 次请求的统计：

| syscall | 次数 | 每次请求 | 来源 |
|---|---|---|---|
| futex | 32,273 | **4.03** | promise 的 2 对睡眠-唤醒 |
| write | 32,158 | **4.02** | 客户端 eventfd（send）+ 客户端 eventfd（定时器）+ 客户端 socket + 服务端 socket |
| epoll_wait | 24,403 | 3.05 | 两个 loop |
| readv | 16,030 | 2.00 | 服务端读 + 客户端读 |
| read | 8,299 | 1.04 | eventfd 读 |
| **合计** | **113,187** | **14.15** | — |

**关键结论**：14 次里只有 4 次真正传数据（2 readv + 2 socket write），**7 次是线程间搬运**（4 futex + 2 eventfd 写 + 1 eventfd 读）。

### 3.4 第四步：CPU 账（确认不是 CPU 瓶颈）

`/usr/bin/time`，80,000 次请求：

| 服务端线程 | user | sys | 每请求 CPU |
|---|---|---|---|
| 1 | 1.44 s | 4.92 s | 79.5 µs（sys 61.5） |
| 8 | 1.45 s | 6.04 s | **93.6 µs（sys 75.5）** |

**结论**：16 vCPU 只用了约 1 个核，sys : user ≈ 4 : 1。
**成本主要花在进内核和线程调度上，不是在 protobuf / CRC / 拷贝上。**

> 这也解释了为什么"优化序列化、减少 malloc"当前收效有限——用户态只占 20%。

### 3.5 第五步：换测量方式，找真实上限

写 `tests/bench_pipeline.cc`，用回调 API + 每连接窗口，在途数 = 连接数 × 窗口。

> 关键实现约束：`handleResponse` 是在**持有 `pendingMutex_` 时**调用回调的，回调里不能再 `asyncCall`（`std::mutex` 不可重入，会自锁）。所以由独立提交线程在窗口有空位时补发。

单连接窗口扫描（10 万请求）：

| 窗口 | QPS | avg |
|---|---|---|
| 1 | 14,841 | 50.7 µs |
| 8 | 77,052 | 79 µs |
| 32 | 205,431 | 113 µs |
| 128 | 285,682 | 305 µs |
| 512 | 345,033 | 907 µs |

**结论**：单连接从 1.48 万涨到 34.5 万——**闭环测出的 41k 单连接上限，本质是"在途=线程数"**。

### 3.6 第六步：内存安全（ASan）

```
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address -g -O1"
```

正常跑 15 次才崩 1 次的问题，**ASan 下 1 次就崩**，并且直接给出分配/释放的完整栈。

**方法论总结（可复用）**：

```
① 校准测量口径（知道自己测的是什么）
    ↓
② 提假设 → 单变量 A/B → 证伪或证实
    ↓
③ 量化成本账（syscall / CPU / 内存）
    ↓
④ 找到真因 → 最小改动 → 同套基准复测
    ↓
⑤ 回归测试 + 内存检查 → 记录数字
```

---

## 4. 第三部分：具体改了什么

### 4.1 目录

| # | 改动 | 类型 | 效果 |
|---|---|---|---|
| 1 | 服务端默认开启 4 个从 Reactor | 配置 | 吞吐 +10%，p99 −26% |
| 2 | Connector 重连加随机抖动 | 健壮性 | 避免同步重连 |
| 3 | TcpClient 所有权声明 | 正确性 | 消除响应串包 |
| 4 | 三层 UAF 修复 | 内存安全 | ASan 20/20 通过 |
| 5 | 调用线程驱动 IO（InlineSyncClient） | 性能 | 吞吐 2.14×，p99 −61% |
| 6 | 关闭 Nagle + 移除空回调 | 性能 | 去掉两处浪费 |
| 7 | 文档一致性 | 工程 | README 与实际一致 |

### 4.2 主从 Reactor 真正生效

```cpp
// examples/rpc/rpc_server_main.cc
RpcServer server(&loop, listenAddr);
server.setThreadNum(4);      // 必须在 start() 之前
server.setIdleTimeout(60);
```

`RpcServer` 补了转发（原来只有 `TcpServer` 有）：

```cpp
// src/server/rpc_server.h
void setThreadNum(int n) { server_.setThreadNum(n); }
```

**踩坑**：`setThreadNum` 在 `start()` 之后调用**不报错、也不生效**——`start()` 里才 `for (i < numThreads_) new EventLoopThread`，之后不会再补建。

### 4.3 重连抖动

```cpp
// src/network/connector.cc
// 半区随机抖动（equal jitter）：delay = base/2 + rand(0, base/2)
int jitterInRange(int low, int high) {
    static thread_local std::mt19937 gen(std::random_device{}());
    if (high <= low) return low;
    std::uniform_int_distribution<int> dist(low, high);
    return dist(gen);
}

// retry() 中：
const int delayMs = jitterInRange(retryDelayMs_ / 2, retryDelayMs_);
loop_->runAfter(delayMs / 1000.0, [weakThis]() { ... });
retryDelayMs_ = std::min(retryDelayMs_ * 2, kMaxRetryDelayMs);
```

实测分布：

| 退避基数 | 区间 | 抽样 |
|---|---|---|
| 500 ms | [250, 500] | 354 / 465 / 404 / 465 / 420 / 400 |
| 8000 ms | [4000, 8000] | 5636 / 4678 / 7796 / 7276 / 7065 / 4092 |

### 4.4 TcpClient 所有权（解决响应串包）

**根因**：一个 `TcpClient` 只有一套回调 + 一条连接，被两个 `RpcAsyncClient` 绑定必然互相覆盖。

**方案**：把"一个 TcpClient 只能被一个上层客户端绑定"变成**硬约束**，冲突时显式失败。

```cpp
// src/network/tcp_client.h
bool tryClaim(const void* owner) {
    const void* expected = nullptr;
    return owner_.compare_exchange_strong(expected, owner);
}
bool isClaimed() const { return owner_.load(std::memory_order_acquire) != nullptr; }
```

```cpp
// src/client/rpc_async_client.cc（构造）
if (!tcpClient_->tryClaim(this)) {
    invalid_ = true;
    ownsClient_ = false;
    tcpClient_.reset();
    loop_ = nullptr;
    return;      // connect() 返回 false，asyncCall() 立即返回错误
}
```

`ConnectionPool::acquire` 跳过已占用连接，全部占用时返回 `nullptr`。

**设计取舍**：与其让两个客户端共享 pending 表（复杂且语义模糊），不如让冲突显式失败——错误信息直接说明原因，不会静默串包。

### 4.5 三层 UAF 修复

**问题 1：owner 在分发过程中死亡**

```cpp
// src/network/channel.h
void tie(const std::shared_ptr<void>& owner) { tie_ = owner; tieSet_ = true; }
std::weak_ptr<void> tie_;
bool tieSet_ = false;
```

```cpp
// src/network/channel.cc
void Channel::handleEvent() {
    std::shared_ptr<void> guard;
    if (tieSet_) {
        guard = tie_.lock();
        if (!guard) return;   // owner 已销毁，跳过
    }
    ... // 正常分发
}   // guard 在这里释放；若这是最后一个引用，对象在函数返回后才析构
```

```cpp
// src/network/tcp_connection.cc
channel_->tie(shared_from_this());
channel_->enableReading();
```

**问题 2：Channel 在自己回调里被销毁**

```cpp
// src/network/connector.cc
int Connector::removeAndResetChannel() {
    ...
    // 不能同步析构：本函数可能正处在 Channel::handleEvent 的栈上
    std::shared_ptr<Channel> doomed(std::move(channel_));
    loop_->queueInLoop([doomed]() { (void)doomed; });   // 本轮 loop 末尾再释放
    return sockfd;
}
```

**问题 3：跨线程裸 `this`**

```cpp
// src/network/tcp_client.cc（closeCallback 用 weak_ptr）
std::weak_ptr<TcpClient> weakSelf = weak_from_this();
conn->setCloseCallback([weakSelf](const TcpConnectionPtr& c) {
    if (auto self = weakSelf.lock()) self->removeConnection(c);
});
```

```cpp
// src/network/connector.cc（start / stop 同样改 weak_ptr）
void Connector::stop() {
    connect_ = false;
    std::weak_ptr<Connector> weakThis = shared_from_this();
    loop_->queueInLoop([weakThis]() {
        if (auto p = weakThis.lock()) p->stopInLoop();
    });
}
```

> `TcpClient` 因此改为继承 `std::enable_shared_from_this<TcpClient>`。

### 4.6 调用线程驱动 IO（本次最大的性能改动）

**核心思想**：同步调用本来就要原地等结果，**不需要第二个线程**。

```
改造前：worker --eventfd--> IO 线程 --write--> 服务端 --read--> IO 线程 --futex--> worker
改造后：worker(==loop) --write--> 服务端 --read--> worker，现场返回
```

**改动一：`EventLoop::loopOnce()`**

```cpp
void EventLoop::loopOnce(int maxWaitMs) {
    int timeoutMs = getNextTimerTimeoutMs();
    if (maxWaitMs >= 0 && maxWaitMs < timeoutMs) timeoutMs = maxWaitMs;
    // epoll_wait + 分发 + processTimers + doPendingFunctors
}

void EventLoop::loop() {
    looping_ = true; quit_.store(false);
    while (!quit_.load()) loopOnce(kEpollWaitTimeoutMs);
    looping_ = false;
}
```

**改动二：`InlineSyncClient`**

```cpp
RpcResponse InlineSyncClient::call(service, method, request, timeoutMs) {
    pendingId_ = nextId_++;
    done_ = false;
    std::string packet = Codec::encodeRequest(...);
    conn->send(packet);          // 本线程就是 loop 线程 → 直接 write()

    const auto deadline = now + timeoutMs;
    while (!done_) {
        int remain = msUntil(deadline);
        if (remain <= 0) return timeout_response();
        loop_.loopOnce(remain);  // 本线程 epoll_wait，响应到达就地解码
    }
    return result_;
}
```

**三个关键设计点**：

1. `EventLoop` 构造在**调用线程**，因此 `isInLoopThread()` 为真，`conn->send` 走直接写；
2. 超时用 `epoll_wait` 的超时参数实现，**不注册任何定时器**；
3. 没有 `promise` / `future` / `futex`，结果就是一个成员变量。

**代价与约束**（必须写清楚）：

- 客户端实例有**线程亲和**，不能跨线程使用；
- 一条连接绑定一个线程，失去"多线程共享连接"；
- 一次只有一个在途请求；要并发得多开连接；
- 在途数远大于 1 时（流水线模式），这部分收益会被摊薄——省的是**每次调用的固定开销**。

### 4.7 两处浪费

```cpp
// src/network/tcp_connection.cc 构造
socket_->setKeepAlive(true);
socket_->setTcpNoDelay(true);   // 之前 setTcpNoDelay 定义了从未调用，Nagle 一直开着
```

```cpp
// src/client/rpc_async_client.cc setupCallbacks
// 不再注册 writeCompleteCallback：onWriteComplete 是空函数，
// 注册了只会让每次 send 多一次 std::bind + 入队。
```

---

## 5. 第四部分：新项目的数据表现

### 5.1 闭环压测（8 线程 × 8 连接，5 万请求）

| 版本 | QPS | avg | p99 |
|---|---|---|---|
| 改动前（单 Reactor） | 56,047 | 141 µs | 626 µs |
| 改动后（4 个从 Reactor） | **61,683** | **128 µs** | **461 µs** |

扩展性（1 → 8 线程 × N 连接）：

| 线程 | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| QPS | 14,297 | 23,272 | 37,126 | **61,683** |
| avg | 70 µs | 86 µs | 106 µs | 128 µs |
| 相对 1 线程 | 1.00× | 1.63× | 2.60× | **4.31×** |

### 5.2 流水线压测（真实吞吐上限）

| 连接 × 在途 | 请求数 | QPS | avg | p99 |
|---|---|---|---|---|
| 1 × 512 | 10 万 | 345,033 | 907 µs | 3.1 ms |
| 8 × 64 | 500 万 | 1,111,995 | 332 µs | 1.9 ms |
| **8 × 64** | **1000 万** | **1,013,298** | 342 µs | 2.2 ms |
| 24 × 64 | 500 万 | **1,382,747** | 881 µs | 4.6 ms |

> **短跑会高估**：200 万请求跑出 116.8 万，1000 万请求稳态只有 101 万（差 13%）。**对外报 101 万。**

服务端从 Reactor 在流水线下的扩展性：

| 从 Reactor | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| QPS | 604,068 | 903,429 | 1,095,795 | **1,133,800** |

> 重要补充：闭环下"1→8 只 +21%"不代表架构无用，而是**根本没压到服务端**。流水线下 1→8 是 +88%。

### 5.3 调用线程驱动 IO 的效果

| 指标 | 跨线程（RpcAsyncClient） | 调用线程（InlineSyncClient） | 变化 |
|---|---|---|---|
| 系统调用 / 请求 | 14.15 | **6.06** | −57% |
| futex / 请求 | 4.03 | **0.01** | ≈归零 |
| write / 请求 | 4.02 | **2.01** | −50% |
| 每请求 CPU | 93.6 µs | **46.7 µs** | −50% |
| QPS（8 线程） | 61,683 | **131,996** | **2.14×** |
| avg 延迟 | 128 µs | **57 µs** | −56% |
| p99 延迟 | 461 µs | **180 µs** | −61% |

稳定性（8 线程，20 万请求 × 3）：126,049 / 132,600 / 122,281 QPS，`Errors: 0`。

### 5.4 内存安全

| 项 | 修复前 | 修复后 |
|---|---|---|
| ASan + `test_connection_pool` × 20 | 1/1 必现 UAF | **20/20 通过** |
| 全量测试 | 12 项 | **13 项全过** |

### 5.5 一句话总结表现

> **同一个框架、同一台机器：闭环 6.17 万 → 流水线 101 万（16.4×）；同步调用重构后系统调用减半、吞吐再翻 2.14 倍；顺带修掉三个内存安全 bug。**

---

## 6. 第五部分：值得讲的技术点

每个点都按「是什么 / 难在哪 / 怎么做的 / 证据」组织。

### 6.1 主从 Reactor + one loop per thread

- **是什么**：主 Reactor 只 accept，连接经 `getNextLoop` 轮询分发到从 Reactor 池，每个从 Reactor 一线程一 epoll。
- **难在哪**：跨线程分发连接要保证生命周期（`queueInLoop` + `shared_ptr` 捕获），以及从 Reactor 数量必须在 `start()` 前确定。
- **证据**：1→8 从 Reactor，流水线下 60.4 万 → 113.4 万。

### 6.2 18B 自描述二进制协议

- **是什么**：`magic(4) + version(1) + msg_type(1) + body_len(4) + req_id(8)`，`body_len` 直接解决粘包，无需外层 Length-Field。
- **难在哪**：字节序、CRC 覆盖范围、非法输入的边界检查。
- **证据**：`kMaxBodyLen`（10MB）/ `kMaxScanBytes`（1024）/ `kMaxBufferSize` 三层上限防 DoS。

### 6.3 非阻塞 Connector + 指数退避 + 随机抖动

- **难在哪**：`connect()` 返回 `-1` 且 `errno == EINPROGRESS` 时，要监听可写事件再用 `getsockopt(SO_ERROR)` 拿真实结果；重连还要处理"对象已析构但定时器还在"。
- **演进**：原来只有退避 → 复盘时补了 equal jitter，才真正解决同步重连。

### 6.4 三个 use-after-free 的定位与修复

- **难在哪**：窄窗口、常规测试抓不到；三处根因各不相同（owner 死亡 / 自发销毁 / 跨线程裸指针）。
- **怎么做的**：ASan 直接给出分配-释放栈；引入 muduo 风格的 `Channel::tie`、延迟销毁、`weak_ptr` 回调。
- **证据**：ASan 20/20。

### 6.5 用 `QPS = 在途 / RTT` 认清测量口径

- **是什么**：闭环压测下，吞吐 = 并发数 / 延迟，所以 4.8 万本质是 165 µs 的 RTT。
- **价值**：这一步之后，所有"优化"都能被正确归因——是降了延迟，还是提了并发。

### 6.6 用 A/B 证伪自己的假设

- **做法**：一次只改一个变量，用同一套基准复测。
- **结果**：两个看似合理的假设（服务端并行度、定时器开销）分别只有 +21% 和 +1.4%。

### 6.7 用 `strace` 建立每请求系统调用账单

- **结果**：14.15 次/请求，其中 7 次是线程间搬运。
- **价值**：把"感觉慢"变成"知道钱花在哪"。

### 6.8 调用线程驱动 IO（最大的一次性能改动）

- **核心洞察**：同步调用不需要第二个线程——调用方本来就要等，完全可以自己跑 event loop。
- **实现要点**：`loopOnce()`、EventLoop 线程亲和、超时走 `epoll_wait` 参数、无 promise/futex。
- **证据**：syscall 14.15→6.06，QPS 2.14×，p99 −61%。

### 6.9 连接池的所有权模型

- **核心洞察**：一个 `TcpClient` 只有一套回调，"共享"在语义上就不成立，应该显式失败而不是静默串包。
- **证据**：4 errors / 3.00 s → 0 errors / 0.84 s。

---

## 7. 第六部分：还有什么不足

诚实清单，按优先级排序。**面试被问"有什么不足"时，从这里挑 2~3 个说，并给出改进方向。**

### 7.1 P1：影响 API 语义或扩展性

| # | 不足 | 现状 | 影响 | 改进方向 |
|---|---|---|---|---|
| 1 | **Future 版 `asyncCall` 是伪异步** | 返回前已 `future.get()` | 使用者无法用 Future 做流水线；"异步客户端"名不副实 | 拆成 `startCall()` + `finishCall()`，promise 交给响应回调置值 |
| 2 | **拦截器一开，回调版也退化** | `chain_.execute` 里调 `doCall` | 熔断/限流/Trace 一上，异步变同步 | 拦截器改 `preHandle` 同步 + `postHandle` 放进响应回调 |
| 3 | 每请求超时定时器（async 路径） | 成功不取消 | 规模 O(QPS×timeout)，10 万 QPS 是 30 万节点 | 改 1 ms 扫描在途表，或 timer wheel |
| 4 | `pendingPromises_` 哈希表 + 互斥锁 | 每请求 2 次加锁 | 流水线 101 万 QPS 下会成为下一个瓶颈 | 每 loop 私有表 + `req_id & (N-1)` 槽位 |

### 7.2 P2：工程化与生产能力

| # | 不足 | 说明 |
|---|---|---|
| 5 | 没有 streaming | 只有 unary，不支持 gRPC 的四种调用类型 |
| 6 | 没有鉴权 | 公网部署时谁都能调；只有限流，没有认证 |
| 7 | 业务 handler 跑在 IO 线程 | 慢 handler 会堵同 loop 上所有连接（队头阻塞） |
| 8 | 没有背压 | 在途无上限、`outputBuffer_` 无界，高负载下内存可能涨 |
| 9 | 没有 CI | 13 项测试 + 3 个 bench 靠手跑，没进 CTest / GitHub Actions |
| 10 | 没开 `-Wall -Wextra` | 编译器警告没纳入构建 |
| 11 | 日志靠编译期 `RPC_SILENT` | 本质是"删可观测性换性能"，应改异步日志 + 分级采样 |
| 12 | `Acceptor::handleRead` 一次只 accept 一个 | 连接风暴时排队 |
| 13 | `Channel::handleEvent` 在 RDHUP 时重复调 readCallback | 多一次无谓 `read()` |
| 14 | `EventLoop::events_` 只扩不缩 | 连接数峰值后内存不回收 |

### 7.3 P3：验证环境的局限

| # | 局限 | 影响 |
|---|---|---|
| 15 | 只在 WSL2 单机验证 | WSL2 单次 syscall 约 5.4 µs（原生 Linux 0.3~1 µs），**绝对性能被放大 5~10 倍**，结论方向性成立但绝对值不可直接外推 |
| 16 | client 与 server 同进程同机 | 16~21 个线程抢核，测出的是"机器容量"而非"框架容量" |
| 17 | 只有 echo 业务 | 空 handler，不代表真实业务 QPS |
| 18 | 没有跨语言客户端 | 原简历"跨语言解码一致"不成立，最终**决定不做**，改为"抓包核对字节序" |

### 7.4 已知但暂不修的

- **历史遗留的 `build/libnetwork.a`**：早期提交把编译产物带进了仓库，`.git` 因此约 18 MB。彻底清理需要重写全部历史（所有 hash 变化），收益 < 风险，暂不处理。
- **`test_connection_pool` 的一次偶发 segfault**：已定位并修复（三层 UAF），ASan 20/20 通过。

---

## 8. 第七部分：方法论复盘

### 8.1 五条教训

**① 测量比优化重要。**
在不知道"测的是什么"的情况下优化，等于闭眼开车。4.8 万这个数字没错，错的是它被当成了吞吐上限。

**② 假设大概率是错的。**
我两个最自信的假设（服务端并行度、定时器开销）分别只值 +21% 和 +1.4%。单变量 A/B 是唯一可靠的证伪手段。

**③ "偶发"往往等于"必然，只是窗口很窄"。**
1/15 才崩一次的 bug，ASan 下 1 次必现。工具能把概率问题变成确定问题。

**④ 文档是代码的一部分。**
README 写"主从 Reactor"但代码没开线程池，`network_layer.md` 写"每个连接一个线程"——这些不是"小瑕疵"，而是会被面试官直接抓到的认知缺口。

**⑤ 简历要用"口径"说话。**
"48,765 QPS" 本身不是谎话，但它缺了一句"8 线程 × 8 连接、闭环同步"。加上口径，它就从"吹牛"变成"专业"。

### 8.2 一个可复用的检查清单

优化任何系统前，先回答这五个问题：

1. **我在测什么？** 吞吐还是延迟？在途数是多少？口径是什么？
2. **成本结构是什么？** 每请求几次 syscall、几次分配、几次拷贝、多少 CPU（user/sys 比）？
3. **我的假设是什么？** 能不能用一次单变量改动证伪它？
4. **改完怎么归因？** 同一套基准、同一个环境、同一个请求量。
5. **有没有回归？** 功能测试 + 内存检查 + 性能对比，三样都要。

### 8.3 对"简历 vs 现实"的反思

简历是 7 月的快照，代码是现在的状态。**被问到时正确做法不是推翻，而是细化**：

- "表述不准确" → 展开成准确版本，不认错；
- "事实不存在" → 承认，说清实际做了什么；
- "数字是旧口径" → 给新数字，说明为什么变。

见 `docs/interview_resume_defense.md`。

---

## 9. 附录

### 9.1 复现命令

```bash
cd ~/projects/rpc_framework
mkdir -p build && cd build
cmake .. -DRPC_SILENT=ON -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# 闭环压测（8 线程 × 8 连接，5 万请求）
./test_benchmark 50000 8 4 3000 only

# 流水线压测（8 连接 × 64 在途，1000 万请求）
./bench_pipeline 8 64 10000000 4

# 调用线程驱动 IO
./bench_inline_sync 8 200000 4

# 系统调用账单
strace -c -f -e trace=write,read,readv,writev,epoll_wait,futex,eventfd2 \
  ./bench_inline_sync 4 2000 4

# 内存检查
mkdir -p ../build-asan && cd ../build-asan
cmake .. -DRPC_SILENT=ON -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer -g -O1"
make -j$(nproc)
for i in $(seq 1 20); do ASAN_OPTIONS=detect_leaks=0 ./test_connection_pool || break; done
```

### 9.2 数字速查

| 指标 | 数值 | 来源 |
|---|---|---|
| 闭环 QPS / avg / p99 | 61,683 / 128 µs / 461 µs | 8 线程 × 8 连接，5 万请求 |
| 流水线 QPS（对外） | 1,013,298 | 8 连接 × 64 在途，1000 万请求 |
| 流水线峰值 | 1,382,747 | 24 连接 × 64 在途 |
| inline QPS / avg / p99 | 131,996 / 57 µs / 180 µs | 8 线程，5 万请求 |
| syscall / 请求 | 14.15 → 6.06 | strace |
| CPU / 请求 | 93.6 µs → 46.7 µs | /usr/bin/time |
| 服务端线程 1→8（闭环） | +21% | A/B |
| 服务端线程 1→8（流水线） | +88% | A/B |
| 关超时定时器 | +1.4% | A/B |
| 测试 | 13 项 + ASan 20/20 | — |

### 9.3 文件索引

| 文件 | 内容 |
|---|---|
| `docs/bench_results_wsl.md` | 全部实测数据与实验过程（1~13 节） |
| `docs/performance_analysis.md` | 最初的静态分析（含被证伪的判断，保留作对照） |
| `docs/optimization_roadmap.md` | 下一层优化方向（按维度） |
| `docs/interview_prep_rpc.md` | 面试问答脚本 |
| `docs/interview_resume_defense.md` | 已投简历 vs 当前代码的话术 |
| `docs/resume_final.md` | 简历定稿文案 |
| `src/client/inline_sync_client.cc` | 调用线程驱动 IO 的实现 |
| `tests/bench_pipeline.cc` | 流水线压测 |
| `tests/bench_inline_sync.cc` | inline 同步压测 |
| `tests/test_pool_ownership.cc` | 所有权回归测试 |

### 9.4 提交记录

```
ed9f70e  fix(network): 修复三层 use-after-free，关闭 Nagle，移除空写完成回调
fa283cc  fix(pool): TcpClient 所有权声明，修复连接池共享连接导致的响应串包
f628259  perf: 同步调用改为调用线程驱动 IO，系统调用 14→6、吞吐 2.14×
6162cb8  perf(bench): 新增流水线压测 bench_pipeline，实测吞吐上限 101 万 QPS
b6d2bd7  perf: 默认开启 4 个从 Reactor，Connector 重连加随机抖动
```
