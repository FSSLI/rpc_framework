# 连接池：实现与健康检查（面试问答稿）

> 对应代码：`src/pool/connection_pool.*`、`src/network/tcp_client.*`、`src/client/rpc_async_client.*`
> 验证：`tests/test_pool_reuse.cc`（10 条断言）、`tests/test_pool_ownership.cc`（6 条断言）

---

## 一、30 秒版（先给结构，再给关键设计）

> "连接池是**启动时预建 N 条长连接**，按服务端点分组存在 map 里。每条连接由一个 `TcpClient` + 一个 IO 线程管理。
>
> **借出**时轮询找一条『既健康、又没被其他客户端占用』的连接；**占用用 CAS 声明**，防止两个客户端共享同一条连接导致回调互相覆盖。
>
> **归还**时只清回调和所有权、不关闭连接，所以连接可以反复复用。"

---

## 二、2 分钟版（结构 + 流程 + 踩过的坑）

### 1）数据结构

```cpp
ConnectionPool
└── pools_: unordered_map<"host:port", unique_ptr<EndpointPool>>

EndpointPool {
    vector<unique_ptr<EventLoopThread>> loops;   // 每条连接一个 IO 线程
    vector<shared_ptr<TcpClient>>       clients; // 每条连接一个 TcpClient
    atomic<size_t>                      nextIndex;  // 轮询游标
}
```

- **一个池 = 一个服务端点**；多服务就多个 key。
- **一条连接 = 一个 `TcpClient` + 一个 `EventLoopThread`**，两个 vector 下标一一对应。

### 2）建池：预建 + 等就绪

对每条连接：

1. 起 `EventLoopThread`，拿到 `EventLoop`；
2. `new TcpClient(loop, serverAddr)`，开启 `retryOnDisconnect`；
3. 用 `std::promise` 等连接结果（3 秒超时）；
4. 成功后清掉这个临时回调（防止池析构时访问已释放的 promise）；
5. 存进两个 vector。

**任何一条失败 → 整池创建失败**，保证池里拿不到坏连接。

### 3）借出：轮询 + 跳过占用 + 健康检查

```cpp
for (attempt < n) {
    idx = nextIndex.fetch_add(1) % n;             // 轮询
    if (clients[idx]->isClaimed()) continue;      // 被别人占用 → 跳过
    conn = clients[idx]->connection();
    if (conn && conn->connected()) return client; // 健康且空闲 → 给
    if (!fallback) fallback = client;             // 空闲但断线 → 兜底
}
if (fallback) fallback->ensureConnected();        // 兜底连接主动触发重连
return fallback;                                  // 全被占用 → nullptr
```

### 4）占用：CAS 所有权（这是踩过坑的地方）

**问题**：`TcpClient` 只有一条连接、一套回调。两个 `RpcAsyncClient` 拿到同一个 `TcpClient` 时，后者的 `setupCallbacks()` 会覆盖前者 → 响应投递给错误的客户端 → `reqId` 匹配不到 → 一直等到超时。

**实测**：8 线程 / 4 连接 → **4 个错误、耗时卡在 3.00 秒**。

**方案**：把"独占"变成硬约束：

```cpp
// 借出方
bool tryClaim(const void* owner) { return owner_.compare_exchange_strong(nullptr, owner); }

// RpcAsyncClient 构造时
if (!tcpClient_->tryClaim(this)) { invalid_ = true; /* 后续调用立即返回错误 */ }

// 析构时
claimed->releaseClaim(this);
```

冲突从"静默串包"变成"显式失败 + 明确错误信息"。

### 5）归还：只清回调，不关连接

```cpp
// RpcAsyncClient::disconnect()
if (ownsClient_) {
    tcpClient_->clearCallbacks();                 // 清 TcpClient + TcpConnection 的回调
    if (!borrowedTransport_) {                    // 自建连接才彻底关闭
        tcpClient_->disconnectPermanently();
    }
    // 池借出的连接：保留，交给池复用
}
```

> 改造前这里无脑 `disconnectPermanently()`，导致**客户端一析构连接就死**，池"越用越少"。
> 现在用 `borrowedTransport_` 区分"自建"和"池借出"，后者归还时不关连接。

### 6）生命周期：析构顺序是硬要求

```cpp
struct EndpointPool {
    vector<...EventLoopThread> loops;    // 声明在前
    vector<...TcpClient>       clients;  // 声明在后
};
```

C++ 反序析构 → **clients 先死，loops 后死**。这是故意的：`~TcpClient()` 要往 loop 线程投递清理任务，所以 loop 必须还活着。

另外 `TcpClient` 的 `closeCallback_` 用 `weak_ptr` 绑定，避免主线程析构 `TcpClient` 后 IO 线程还回调裸 `this`（这是修掉的三个 UAF 之一）。

---

## 三、健康检查怎么做的

**分三层，别只说"检查 connected"就完了。**

### 第 1 层：借出时检查状态位（被动）

```cpp
auto conn = client->connection();     // 加锁取快照，可能为 null
if (conn && conn->connected()) ...
```

- `TcpClient` 只是"连接的管理者"，`connection_` 在断线时会被 reset → 可能是 `nullptr`；
- 所以不能假设"池里的连接一定活着"，每次借出都要确认。

### 第 2 层：运行中自动恢复（Connector）

```cpp
// TcpClient::removeConnection()
if (retryOnDisconnect_ && !disconnecting_.load()) {
    connector_->restart();     // 500ms → 30s 指数退避 + 随机抖动
}
```

网络抖动、服务端重启这类断线，**不需要池介入**，Connector 自己会重连。

### 第 3 层：内核与对端兜底

| 机制 | 作用 |
|---|---|
| `SO_KEEPALIVE` | 内核探测对端是否存活（默认 2 小时级） |
| 服务端 idle 超时 | 每 5 秒扫一遍连接，60 秒没 I/O 就主动关闭（回收僵尸连接） |
| 请求超时 | 每个请求 3~5 秒兜底，保证调用方不会永久阻塞 |

### 没有做的（要诚实说）

> **没有做主动的 RPC 级探活（ping）。**

半开连接（对端断电、NAT 表项失效，本地 socket 还是 ESTABLISHED）只能靠 keepalive 和业务超时兜底，发现得慢。要做的话可以利用现有的 `Heartbeat` 报文：定时发心跳、N 次无响应就把连接标记为不健康并强制重建。

---

## 四、高频追问防御

| 追问 | 怎么答 |
|---|---|
| **"连接池怎么判断连接可用？"** | 借出时查 `connection() && connected()`；断线由 Connector 自动重连；借到断线连接会主动 `ensureConnected()` 触发重连 |
| **"怎么发现半开连接？"** | 诚实：没有主动探活，靠 `SO_KEEPALIVE` + 请求超时 + 服务端 idle。要改进就用 Heartbeat 做定时探活 |
| **"池大小怎么定？"** | 一般等于并发客户端数（每个线程一条）；或按 `Little's law`：目标 QPS × RTT 估算在途数 |
| **"为什么一条连接一个线程？"** | one loop per thread 实现简单、无锁；但连接数大时是瓶颈（1 万连接 = 1 万线程），要改成多连接共享一个 loop |
| **"acquire 线程安全吗？"** | 是：`mutex_` 保护 map 与游标，`nextIndex` 和 claim 都是原子操作 |
| **"连接断了但没人用，池知道吗？"** | 不需要知道：Connector 会自己重连；借出时再检查一次状态 |
| **"池会不会把同一条连接给两个人？"** | 不会：CAS 所有权 + acquire 跳过已占用；冲突时显式失败（有回归测试） |
| **"客户端析构后连接还能用吗？"** | 能：只清回调 + 释放所有权，不关连接（`test_pool_reuse` 专门验证这点） |

---

## 五、数据支撑（讲的时候带上）

| 指标 | 数值 |
|---|---|
| 单连接吞吐上限 | ~41k QPS（瓶颈是单连接 + 单客户端 loop） |
| 8 连接吞吐 | 61.7k QPS（+50%） |
| 改造前：8 线程 / 4 连接 | **4 errors / 3.00 s / 16,658 QPS** |
| 改造后：同场景 | **0 errors / 0.84 s / 59,852 QPS** |
| 复用测试 | 10 条断言全过（析构后连接存活、二次借用同一条连接、显式 release 后可用） |

---

## 六、一句话收尾

> **这个池解决的是"建连成本 + 并发槽位"，不是吞吐上限；健康检查是"借出时查状态 + 运行时自动重连 + 内核 keepalive"三层兜底，主动探活是已知的未做项。**
