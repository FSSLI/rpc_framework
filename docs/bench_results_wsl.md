# 实测报告：WSL2 上的瓶颈定位（2026-10-30）

> 环境：Intel i7-13620H（8 核 16 线程，WSL2 分配 16 vCPU / 7 GB）
> 构建：`cmake .. -DRPC_SILENT=ON -DCMAKE_BUILD_TYPE=Release`，`g++ 11.4`、`protoc 3.12.4`
> 代码：WSL 内 `~/projects/rpc_framework`（比 D 盘那份新 2 个提交）
> 方法：改动 benchmark 加入三个可调参数（服务端线程数 / 超时 / 只跑 scaling），逐项单变量对比

---

## 0. 一句话结论

**这个框架现在既不是服务端 CPU 瓶颈，也不是定时器瓶颈，而是"每请求 14 次系统调用 + 只有 8 个在途请求"的延迟瓶颈。**

- 服务端 IO 线程从 1 开到 8：QPS 只 +21%（51,240 → 61,901）
- 关掉每请求超时定时器：QPS 只 +1.4%（57,771 → 58,594）
- 全系统 16 核只用了约 1 个核（80,000 请求共 7.49s CPU）
- 每请求 14.15 次 syscall、93.6 µs CPU（其中 75.5 µs 在内核态）
- `QPS = 在途数 / RTT` 精确成立：8 / 153 µs = 52.3k（实测 51.2k）、8 / 125 µs = 64k（实测 61.9k）

---

## 1. 基线复现（对照 README）

`./test_benchmark 20000 8`，8 连接 / 8 线程：

| 并发 | 实测 QPS | 实测 avg | 实测 p99 | README QPS | 相对 1 线程 |
|---|---|---|---|---|---|
| 1 | 13,043 | 76 µs | 191 µs | 11,842 | 1.00× |
| 2 | 23,371 | 85 µs | 359 µs | 19,795 | 1.79× |
| 4 | 37,452 | 106 µs | 397 µs | 34,397 | 2.87× |
| 8 | **51,592** | **152 µs** | **699 µs** | 48,442 | **3.96×** |

结论：README 的数字**可复现**（绝对值高 5~13%，扩展比 3.96× vs 4.09×，同一台机器上属正常波动）。

---

## 2. 实验一：服务端 IO 线程数（证伪"单 Reactor 是主要瓶颈"）

给 `RpcServer` 补了 `setThreadNum()` 转发（原来只有 `TcpServer` 有，`RpcServer` 没暴露），benchmark 加第 3 个参数。

`./test_benchmark 20000 8 <serverThreads> only`，8 连接：

| 服务端线程 | QPS | avg | p99 | 相对 |
|---|---|---|---|---|
| 1 | 51,240 | 153 µs | 833 µs | 1.00× |
| 2 | 55,224 | 140 µs | 693 µs | 1.08× |
| 4 | 57,111 | 136 µs | 634 µs | 1.11× |
| 8 | **61,901** | **125 µs** | **508 µs** | **1.21×** |

> **术语精确化**：`serverThreads=1` 已经是"主从 Reactor + 1 个从 Reactor"（主 Reactor 只 accept，从 Reactor 管全部连接），不是严格的单 Reactor。严格的单 Reactor 是**默认值 0**（不创建从 Reactor，连接全落主 Reactor），那一组的 8 连接数据是 **51,592 QPS / 152 µs**，与 `serverThreads=1` 的 51,240 / 153 µs 几乎一致——说明 accept 不在关键路径上。

**结论：服务端并行度不是吞吐瓶颈。** 1 → 8 个从 Reactor 只让吞吐 +21%，主要改善的是延迟（p99 833 → 508 µs）。

⚠️ 这与我之前"服务端单 Reactor 是第一瓶颈、开线程池能 2~4×"的判断**不符**，以实测为准。

---

## 3. 实验二：每请求超时定时器（证伪"定时器是主要开销"）

`timeout_ms=0` 时 `doCall` 里的 `if (timeout_ms > 0 && timeoutLoop)` 不成立，`runAfter` 完全不执行。

`./test_benchmark 20000 8 8 <timeout> only`：

| 超时 | QPS | avg | p99 |
|---|---|---|---|
| 3000 ms（每请求一个定时器） | 57,771 | 135 µs | 565 µs |
| 0（不注册定时器） | 58,594 | 133 µs | 557 µs |

**结论：只差 1.4%。** 定时器确实在做无用功（每请求一次 eventfd 写 + 堆插入），但在 14 次系统调用里只占 1 次，**不是主要成本**。

⚠️ 这也修正了我之前的判断。它的价值在于"规模会随 QPS×timeout 增长"（10 万 QPS 时是 30 万个节点），属于**可扩展性隐患**，而不是当前瓶颈。

---

## 4. 实验三：系统调用账单（真正的瓶颈）

```
strace -c -f -e trace=write,read,readv,writev,epoll_wait,futex,eventfd2 \
  ./test_benchmark 2000 4 8 3000 only
```

8,000 次请求的统计：

| syscall | 次数 | 每次请求 | 来源 |
|---|---|---|---|
| futex | 32,273 | **4.03** | promise/worker 的 2 对睡眠-唤醒 |
| write | 32,158 | **4.02** | ①客户端 send 的 eventfd 唤醒 ②超时定时器的 eventfd 唤醒 ③客户端 socket 写 ④服务端 socket 写 |
| epoll_wait | 24,403 | 3.05 | 客户端 loop、服务端 loop |
| readv | 16,030 | 2.00 | 服务端读、客户端读 |
| read | 8,299 | 1.04 | eventfd 读 |
| **合计** | **113,187** | **14.15** | — |

**关键观察：14 次系统调用里，只有 4 次是真正传数据（2×readv + 2×socket write），另外 7 次（2 次 eventfd 写 + 1 次 eventfd 读 + 4 次 futex）全是"把一个请求从线程 A 搬到线程 B"的开销。**

---

## 5. 实验四：CPU 账

`/usr/bin/time -f "cpu user=%Us sys=%Ss"`，80,000 次请求：

| 服务端线程 | user | sys | wall | 每请求 CPU |
|---|---|---|---|---|
| 1 | 1.44 s | 4.92 s | 8.00 s | 79.5 µs（sys 61.5） |
| 8 | 1.45 s | 6.04 s | 7.50 s | **93.6 µs（sys 75.5）** |

两个推论：

1. **16 个 vCPU 只用了约 1 个**。CPU 远没饱和，所以"加线程/加服务端并行"不会有明显收益——实验一已经证实。
2. **sys : user ≈ 4 : 1**。这说明成本主要在进内核和线程调度上，而不是在 protobuf、CRC、拷贝这些用户态逻辑上。这也解释了为什么"优化序列化/减少 malloc"在当前阶段收效有限。

按 93.6 µs/请求、8~16 个可用核折算，**这台机器在本实现下的吞吐上限约 8.5 万 ~ 17 万 QPS**（推算，未验证）。当前 6.2 万，也就是还有 1.4~2.7 倍空间，而且必须先减少每请求系统调用才拿得到。

---

## 6. 顺带验证的一个真 bug

`Bench 2: Pool (4 conns, 8 threads)` 实测：

```
Requests: 20000  Errors: 4  Elapsed: 3.00s  QPS: 6659/s
```

- **4 个错误**、耗时恰好 3.00 s（= 超时时间）；
- 原因就是之前推断的：`ConnectionPool::acquire` 把同一个 `TcpClient` 分给两个 `RpcAsyncClient`，后者构造时 `setupCallbacks()` 把连接的回调改成指向自己 → 一半响应被投递给错误的 client → 匹配不到 reqId → 3 秒后超时；
- 顺带还发现：`timeout=0` 时这条路径会**永久阻塞**（没有超时就永远等），说明"必须有超时"是当前设计的硬约束。

这也说明 README 里"8 线程 4 连接 48,765 QPS、0 错误"的配置在**当前代码下不可能成立**。

---

## 7. 修正后的优化优先级

| 优先级 | 动作 | 消除的系统调用 | 预期 |
|---|---|---|---|
| **P0** | **同步调用改为"在调用线程里跑 loop"** | -2 次 eventfd 写、-4 次 futex（14 → 8） | RTT 有望 133 → 80~90 µs，QPS +40~60% |
| **P0** | **真异步 + pipeline（每连接 64 在途）** | readv/epoll_wait 被摊薄 | 在途数不再等于线程数，吞吐可逼近 CPU 上限 |
| P1 | 响应写合并 / `writev` | write 从 4 → 2（批内摊薄到 0.1） | 抬高极限 |
| P1 | 每 loop 私有 pending 表（无锁） | -2 次 futex 竞争 | 在途上千后必需 |
| P2 | 每请求定时器 → 1 ms 扫描 | -1 次 eventfd 写 | 当前 +1.4%，但去掉 O(QPS×timeout) 的隐患 |
| P2 | 零拷贝 decode、tcmalloc、CRC slice-by-8 | - | user 侧只占 20%，收益上限有限 |
| P3 | 修 `benchPool` 共享连接 bug | - | 正确性 |

**对之前判断的修正**：

| 之前的判断 | 实测结果 | 修正 |
|---|---|---|
| 服务端单 Reactor 是第一瓶颈 | +21% 而非 2~4× | 降级为"延迟优化项"，不是吞吐瓶颈 |
| 每请求定时器是最大浪费 | +1.4% | 降级为"可扩展性隐患"，不是当前瓶颈 |
| 每请求 2 次 eventfd + 1 次 futex | 实测 4 write / 4 futex | **证实**，且量化到 14 次 syscall |
| QPS = 在途 / RTT | 误差 2~3% | **证实** |
| 线程数 > 连接数会导致回调覆盖 | 4 errors / 3.00s | **证实** |

---

## 8. 复现命令

```bash
cd ~/projects/rpc_framework/build
# 基线
./test_benchmark 20000 8
# 服务端线程数对比（第 3 个参数）
./test_benchmark 20000 8 1 only
./test_benchmark 20000 8 8 only
# 超时开关（第 4 个参数，0 = 不注册定时器）
./test_benchmark 20000 8 8 0 only
# 系统调用账单
strace -c -f -e trace=write,read,readv,writev,epoll_wait,futex,eventfd2 \
  ./test_benchmark 2000 4 8 3000 only
```

> 本次为验证改动了两处源码（未提交）：
> - `src/server/rpc_server.h`：新增 `setThreadNum(int)` 转发（原本缺失）
> - `tests/test_benchmark.cc`：新增 3 个可选参数（服务端线程数 / 超时 / 只跑 scaling）

---

## 9. 落地改动与验证（2026-10-30）

### 9.1 Connector：指数退避 + 半区随机抖动

`src/network/connector.cc`，新增匿名命名空间的抖动函数，`retry()` 里用抖动后的延迟替代固定退避：

```cpp
// 半区随机抖动（equal jitter）：delay = base/2 + rand(0, base/2)
// 目的：大量客户端同时断线时，避免它们在同一时刻同步重连（重连风暴）
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

实测分布（抽样 6 次）：

| 退避基数 | 抖动区间 | 抽样结果 |
|---|---|---|
| 500 ms（第 1 次） | [250, 500] | 354 / 465 / 404 / 465 / 420 / 400 |
| 8000 ms（第 5 次） | [4000, 8000] | 5636 / 4678 / 7796 / 7276 / 7065 / 4092 |

这样才配得上"避免重连风暴"——**没有 jitter 的指数退避只降低单连接频率，不解决同步重连**。

### 9.2 服务端默认开启 4 个从 Reactor

```cpp
// examples/rpc/rpc_server_main.cc
RpcServer server(&loop, listenAddr);
server.setThreadNum(4);      // 主从 Reactor：4 个从 Reactor（必须在 start() 之前）
server.setIdleTimeout(60);
```

`tests/test_benchmark.cc` 的第 3 个参数默认值同步改为 **4**，保证压测口径与生产 demo 一致。

### 9.3 最终基准（默认 4 个从 Reactor，5 万请求）

| 场景 | QPS | avg | p99 |
|---|---|---|---|
| Direct（1 连接 / 8 线程） | 41,051 | 188 µs | 724 µs |
| Scaling（8 连接 / 8 线程） | **61,683** | **128 µs** | **461 µs** |

对比改动前（单 Reactor、5 万请求）：8 连接 56,047 QPS / avg 141 µs / p99 626 µs。
**吞吐 +10%，p99 −26%。**

### 9.4 回归测试

12 项全部通过：`test_connector`、`test_consistent_hash`、`test_consistent_hash_lb`、`test_memory_registry`、`test_service_discovery_e2e`、`test_connection_pool`、`test_load_balance`、`test_circuit_breaker`、`test_token_bucket`、`test_trace_id`、`test_interceptor`、`test_integration`。

### 9.5 仍未修复（演示前注意）

- `Bench 2: Pool (4 conns, 8 threads)` 仍然 **4 errors / 3.00 s**：同一 `TcpClient` 被两个 `RpcAsyncClient` 共享，回调被后者覆盖。**演示连接池时用 8 连接 / 8 线程（Scaling 那组），不要用 4 连接 / 8 线程。**
- README 里的 `48,765 QPS / p99 382µs / +60%` 已与当前代码不符，需要按上表更新。

---

## 10. 流水线压测：真实吞吐上限（2026-10-30 晚）

### 10.1 方法

新增 `tests/bench_pipeline.cc`，用**回调 API**（真异步路径）让每条连接维持 `window` 个在途请求，在途数 = `conns × window`，可远大于线程数。

> 关键约束：`handleResponse` 是在**持有 `pendingMutex_` 的情况下**调用回调的，
> 所以回调里绝对不能再调 `asyncCall`（`std::mutex` 不可重入，会自锁）。
> 本实现由独立提交线程在窗口出现空位时补发，回调只做"记录延迟 + 计数 + 通知"。

### 10.2 单连接窗口扫描（100,000 请求，服务端 4 从 Reactor）

| 每连接在途 | QPS | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) |
|---|---|---|---|---|---|
| 1 | 14,841 | 50.7 | 49.6 | 169.1 | 837.0 |
| 8 | 77,052 | 79.0 | 67.1 | 392.1 | 1,070.7 |
| 32 | 205,431 | 113.1 | 84.8 | 882.4 | 1,832.0 |
| 128 | 285,682 | 304.9 | 225.5 | 1,461.7 | 2,021.1 |
| 512 | 345,033 | 906.7 | 788.7 | 3,076.1 | 5,435.1 |

单连接从 1.48 万涨到 34.5 万（23×）——**闭环测出的 41k 单连接上限，本质是"在途=线程数"，不是连接本身的上限**。

### 10.3 多连接（500,000 请求，服务端 4 从 Reactor）

| 连接 | 在途/连接 | 总在途 | QPS | avg (µs) | p99 (µs) | CPU (user/sys) |
|---|---|---|---|---|---|---|
| 1 | 1024 | 1,024 | 450,171 | 1,687 | 3,178 | 1.26 / 1.40 s |
| 1 | 2048 | 2,048 | 443,702 | 3,321 | 8,007 | 1.39 / 1.33 s |
| 4 | 256 | 1,024 | 926,235 | 649 | 2,203 | 2.05 / 2.32 s |
| 8 | 64 | 512 | 965,175 | 355 | 1,857 | 2.59 / 2.86 s |
| 8 | 256 | 2,048 | **1,148,599** | 1,077 | 4,304 | 2.38 / 2.40 s |

单连接加压到 1024 后饱和在 45 万（客户端单 loop 上限）；**多连接才能把 CPU 铺到多核**。

### 10.4 稳定性（8 连接 × 64 在途，200 万请求，重复 3 次）

| 次数 | QPS | avg (µs) | p99 (µs) | p999 (µs) | 错误 |
|---|---|---|---|---|---|
| 1 | 1,167,922 | 301 | 1,492 | 3,597 | 0 |
| 2 | 1,107,274 | 328 | 1,879 | 4,551 | 0 |
| 3 | 1,144,195 | 315 | 1,785 | 3,831 | 0 |

CPU 约 9.5~10.1 s user + 11.1~11.4 s sys，wall 2.13~2.14 s → **约 9.6 / 16 核**，尚未跑满。

### 10.5 服务端从 Reactor 在流水线下重新成为瓶颈（8×64，200 万请求）

| 从 Reactor 数 | QPS | avg (µs) | p99 (µs) |
|---|---|---|---|
| 1 | 604,068 | 734 | 2,017 |
| 2 | 903,429 | 437 | 1,839 |
| 4 | 1,095,795 | 329 | 1,936 |
| 8 | **1,133,800** | 307 | 1,710 |

**这是对第 2 节结论的重要补充**：闭环下"服务端 1→8 线程只 +21%"，是因为客户端在途数只有 8、根本没压到服务端；
流水线下 1→8 从 60.4 万涨到 113.4 万（1.88×）。**主从 Reactor 的扩展性是真的，只是之前测不出来。**

### 10.6 长稳态与饱和点（收尾实验）

**同一配置，短跑 vs 长稳态（8 连接 × 64 在途）：**

| 请求数 | QPS | avg (µs) | p99 (µs) | p999 (µs) | CPU |
|---|---|---|---|---|---|
| 2,000,000 | 1,167,922 | 301 | 1,492 | 3,597 | ~9.6 核 |
| 10,000,000 | **1,013,298** | 342 | 2,197 | 6,161 | 130.4 s / 11.2 s = **11.6 核** |

长稳态比短跑低约 13% —— **短跑会高估吞吐**，对外报数应以 10M 的 **101 万** 为准。
10M 稳态下每请求 CPU 13.0 µs（user 6.8 + sys 6.3），**sys 占 48%**。

**连接数扫描（500 万请求，每连接 64 在途）：**

| 连接数 | QPS | avg (µs) | p99 (µs) | CPU |
|---|---|---|---|---|
| 8 | 1,111,995 | 332 | 1,907 | 10.1 核 |
| 12 | 1,138,428 | 500 | 2,715 | 10.2 核 |
| 16 | 1,243,051 | 632 | 3,445 | 10.6 核 |
| 24 | **1,382,747** | 881 | 4,578 | 11.0 核 |

三个观察：

1. 吞吐随连接数**单调上升**，24 连接仍未饱和；
2. CPU 稳定在 10~11 核（共 16 核），**说明瓶颈不是 CPU 总量**，更像系统调用/内核路径 + 单连接 loop 的串行度；
3. 延迟随连接数明显上升（332 → 881 µs），那是**排队**，不是单次请求变慢。

**"天花板在哪"的答案**：16 vCPU 的 WSL2 上，本实现约 **100~140 万 QPS**，再往上被每请求 ~13 µs 的 CPU（近一半在核态）卡住。要继续提升，必须减少每请求系统调用，而不是加线程/加连接。

### 10.7 结论

1. **同一个框架，闭环 61,683 QPS → 流水线长稳态 1,013,298 QPS（16.4×），峰值 1,382,747（22×）**。测量方式决定数字，README 里的"48,765 QPS"是延迟口径。
2. 延迟随在途数近似线性上升（`QPS ≈ 在途 / RTT`），**流水线下的 p99 含排队，不能和闭环 p99 对比**。
3. 16 vCPU 用了 10~11.6 核，**吞吐还没到 CPU 总量上限**；继续加压需要更多连接（客户端 loop 数）而非更大窗口。
4. 对外报数用 **长稳态 101 万**（10M 请求），不要用 2M 短跑的 116.8 万——短跑高估约 13%。
4. 服务端并行度在流水线下有效（+88%），此前的"A/B 实验证伪"应理解为**测量窗口太窄**，不是架构无用。
5. 该数字是空业务 echo，**不代表真实业务 QPS**；真实业务受 handler 耗时限制。

### 10.8 复现

```bash
cd ~/projects/rpc_framework/build
make -j16 bench_pipeline
./bench_pipeline 8 64 2000000 4     # 8 连接 × 64 在途 × 200 万请求 × 服务端 4 从 Reactor
./bench_pipeline 1 512 100000 4     # 单连接窗口扫描
```

---

## 11. 优化验证：同步调用改为"调用线程驱动 IO"（2026-10-30 深夜）

### 11.1 改了什么

| 文件 | 改动 |
|---|---|
| `src/network/event_loop.h/.cc` | 新增 `loopOnce(maxWaitMs)`：只做一轮 `epoll_wait` + 定时器 + pending functors；`loop()` 改为 `while (loopOnce())` |
| `src/client/inline_sync_client.h/.cc`（新增） | `InlineSyncClient`：`EventLoop` 构造在**调用线程**，`send` 直接 `write()`，等待响应时在本线程反复 `loopOnce()` |
| `tests/bench_inline_sync.cc`（新增） | 对照压测：每线程一个客户端 + 一条连接 |
| `CMakeLists.txt` | 加入新源文件与 bench 目标 |

关键点：**没有 promise、没有 futex、没有 eventfd**。超时判断改成"把剩余时间作为 `epoll_wait` 的超时参数"，连每请求的超时定时器都省掉了。

### 11.2 系统调用（`strace -c -f`，2000 请求 / 4 线程）

| syscall | 优化前（跨线程） | 优化后（调用线程） |
|---|---|---|
| futex | 4.03 | **0.01** |
| write | 4.02 | **2.01** |
| epoll_wait | 3.05 | 2.02 |
| readv | 2.00 | 2.00 |
| read | 1.04 | **0.01** |
| **合计 / 请求** | **14.15** | **6.06（−57%）** |

4 次 write 变成 2 次，是因为"客户端 eventfd 唤醒"和"定时器 eventfd 唤醒"都消失了，只剩客户端与服务端各一次真正的 socket 写。
4 次 futex 变成 0.01，是因为 promise/future 的睡眠-唤醒整条链路被删除。

### 11.3 性能（8 线程 × 8 连接，5 万请求，服务端 4 从 Reactor）

| 版本 | QPS | avg (µs) | p99 (µs) |
|---|---|---|---|
| 优化前：`RpcAsyncClient` + `future.get()` | 61,683 | 128 | 461 |
| 优化后：`InlineSyncClient` | **131,996** | **57** | **180** |
| 变化 | **2.14×** | **−56%** | **−61%** |

扩展性（5 万请求）：

| 线程 | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| QPS | 24,862 | 42,842 | 68,204 | **131,996** |
| avg (µs) | 40.0 | 45.1 | 57.8 | 56.7 |

稳定性（8 线程，20 万请求 × 3）：**126,049 / 132,600 / 122,281 QPS**，avg 59~64 µs，p99 170~205 µs，`Errors: 0`。

### 11.4 代价与约束（必须写清楚）

1. **线程亲和**：`EventLoop` 构造在调用线程，客户端实例不能跨线程使用；
2. **一连接一线程**：每个工作线程独立连接，失去"多线程共享一条连接"的能力；
3. **串行**：一次只有一个在途请求；要并发得多开连接（或改造成窗口模式）；
4. 当在途请求数远大于 1 时（流水线模式），这部分收益会被摊薄——省的是固定的每次调用开销。

### 11.5 结论

- 每请求系统调用 **14.15 → 6.06**，其中 futex **4.03 → 0.01**；
- 同配置吞吐 **2.14×**，p99 **−61%**；
- 这正面验证了第 5 节的判断：**瓶颈是同步调用的线程跳转，不是服务端并行度**；
- 也验证了"改测量方式（流水线）"与"改实现（调用线程驱动）"是两条独立的优化路径，二者可以叠加。

---

## 12. 修复：连接池把同一条连接交给两个客户端（2026-10-30）

### 12.1 现象

`Bench 2: Pool`（8 线程 / 4 连接）：

```
Requests: 20000  Errors: 4  Elapsed: 3.00s  QPS: 6659/s
```

- **4 个错误**，耗时恰好 **3.00 s**（= 超时时间）；
- `timeout=0` 时该路径会**永久阻塞**（没有超时兜底）。

### 12.2 根因

1. `ConnectionPool::acquire` 轮询返回 `TcpClient`，**线程数 > 连接数时会把同一条连接交给两个客户端**；
2. `RpcAsyncClient` 池模式构造时调用 `setupCallbacks()`，把 `TcpClient` 与 `TcpConnection` 的 3 个回调**覆盖**为指向自己；
3. 于是后构造的客户端"偷走"了先构造者的响应：`reqId` 在 A 的 pending 表里查不到 → 一直等到 3 s 超时。

### 12.3 修复

| 位置 | 改动 |
|---|---|
| `network/tcp_client.h` | 新增所有权声明：`tryClaim()` / `releaseClaim()` / `isClaimed()`（CAS 抢占） |
| `client/rpc_async_client` | 构造时 `tryClaim(this)`；失败则置 `invalid_`，`connect()` 返回 false，`asyncCall()` **立即**返回 `"TcpClient already owned by another RpcAsyncClient"`；析构时 `releaseClaim`；`disconnect()` 不再断开别人持有的连接 |
| `pool/connection_pool.cc` | `acquire` 跳过已占用连接；全部被占用时返回 `nullptr`（而不是把已占用的交出去） |
| `tests/test_pool_ownership.cc`（新增） | 6 条断言的回归测试 |
| `tests/test_benchmark.cc` | `benchPool` 的连接数改为 ≥ 线程数 |

设计取舍：**"一个 TcpClient 只能被一个上层客户端绑定"是硬约束**。与其让两个客户端共享 pending 表（复杂且语义模糊），不如让冲突**显式失败**——错误信息直接说明原因，不会静默串包。

### 12.4 验证

| 场景 | 修复前 | 修复后 |
|---|---|---|
| Pool 8 线程 × 8 连接，5 万请求 | 4 errors / 3.00 s / 16,658 QPS | **0 errors / 0.84 s / 59,852 QPS** |
| 全量测试（13 项） | — | **13/13 通过** |
| `test_connection_pool` 连续 10 次 | — | **10/10 通过** |

> 遗留观察：在一次连续跑全量测试时，`test_connection_pool` 出现过 **1 次 segfault**（发生在测试 1 的池析构、连接器重连 `SO_ERROR=111` 之后）。单独重跑 10 次未复现，gdb 下也未复现。怀疑是原有的"池析构 + 自动重连"竞态，与本次改动路径无关，但**尚未定位**，记录待查。

---

## 13. 三层 use-after-free 的定位与修复（2026-10-30 晚）

第 12.4 节留了个尾巴（偶发 segfault）。用 **AddressSanitizer** 构建后 **1/1 必现**，而且是三个独立的生命周期 bug，共同特征是：**在回调栈上销毁了正在被分发的对象**。

### 13.1 三个 bug

| # | 崩溃点 | 触发链 | 本质 |
|---|---|---|---|
| 1 | `Channel::handleEvent` 读 `revents_` | `handleRead` → `handleClose` → `TcpConnection`（连同 Channel）析构 → 返回后继续读成员 | owner 在分发过程中死亡 |
| 2 | `Channel::handleEvent` 读 `readCallback_` | `errorCallback_` → `Connector::handleError` → `removeAndResetChannel` → Channel 析构 → 返回后继续 | Channel 在自己回调里被销毁 |
| 3 | `TcpClient::removeConnection` 读 `connectionCallback_` | 主线程析构 `TcpClient` → IO 线程之后处理连接关闭 → `closeCallback_` 里裸 `this` 已失效 | 跨线程对象生命周期 |

### 13.2 修复

| 位置 | 改动 |
|---|---|
| `network/channel.h/.cc` | 新增 `tie(weak_ptr<owner>)`；`handleEvent()` 开头 `lock()` 成 `shared_ptr`，owner 在分发期间存活，析构推迟到函数返回后 |
| `network/connector.cc` | `removeAndResetChannel()` 不再同步 `reset`，把 Channel 移交给本轮 loop 末尾的 pending functor 持有 |
| `network/tcp_client.h/.cc` | 继承 `enable_shared_from_this`；`closeCallback_` 改为捕获 `weak_ptr<TcpClient>` |
| `network/connector.cc` | `start()` / `stop()` 投递任务改用 `weak_ptr`，对象销毁后自动跳过 |

### 13.3 验证

| 项 | 结果 |
|---|---|
| ASan + `test_connection_pool` × 20 | **20/20 通过**（修复前 1/1 必现） |
| 全量测试（13 项） | **13/13 通过** |
| 闭环基准（8 连接 / 5 万请求） | 59,457 QPS / p99 465 µs（对比 61,683，正常波动） |
| `bench_inline_sync`（8 线程 / 20 万） | 129,254 QPS / avg 61 µs（对比 131,996，−2%） |

### 13.4 顺带修掉的两处浪费

- `Socket::setTcpNoDelay` 之前**定义了从未调用** → Nagle 一直开着。已在 `TcpConnection` 构造里启用，避免小包场景 40 ms 级延迟尖刺；
- `RpcAsyncClient::setupCallbacks` 注册了 `writeCompleteCallback`，但 `onWriteComplete()` 是空函数 → 每次 send 多一次 `std::bind` + 入队。已移除注册。

### 13.5 教训

`ASan` 是这类"偶发崩溃"的唯一高效手段：正常跑 15 次才崩 1 次的 bug，ASan 下 1 次就崩，并且直接给出分配/释放的完整调用栈。**"偶发"往往等于"必然，只是窗口很窄"。**
