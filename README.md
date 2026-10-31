# RPC Framework

基于 C++14 的高性能 RPC 框架，支持同步调用、服务注册发现、负载均衡、熔断降级。

## 特性

- [x] 自定义二进制协议（魔数 + 版本 + CRC32 校验）
- [x] Protobuf 序列化
- [x] TCP 粘包处理（固定头 body_len 字段，自描述协议）
- [x] epoll/Reactor 网络模型（主 Reactor accept + 4 个从 Reactor，one loop per thread）
- [x] 多 IO 线程（EventLoopThreadPool 轮询分发连接，从 Reactor 数可配）
- [x] 同步 RPC 客户端（底层复用异步，future.wait_for 超时控制）
- [x] 调用线程驱动 IO 的同步客户端（`InlineSyncClient`：无 eventfd/futex，每请求 6 次系统调用，吞吐 2.14×）
- [x] 异步 RPC 客户端（Future/Promise + Callback 双模式）
- [x] **非阻塞 Connector + 自动重连（指数退避）**
- [x] **TcpClient 客户端底座（基于 Connector）**
- [x] 服务端服务注册 + 方法分发（RpcServer）
- [x] 连接 idle 超时检测（服务端主动断开不活跃连接）
- [x] 一致性哈希负载均衡（MurmurHash + 虚拟节点）
- [x] 轮询负载均衡（多节点自动 failover）
- [x] 服务注册发现（MemoryRegistry 内存 + EtcdRegistry 支持 etcd v3 lease/Watch）
- [x] 连接池（固定大小 + 轮询 + 健康检查，单连接 41k → 8 连接 61.7k QPS，+50%）
- [x] 熔断降级（CLOSED→OPEN→HALF_OPEN 三态，半开单探测）
- [x] 限流（本地令牌桶 + Redis 分布式令牌桶 Lua 原子脚本）
- [x] TraceID 透传（metadata 自动注入，服务端提取日志）
- [x] 拦截器链（责任链模式，熔断/限流/日志/Trace 插件化）
- [x] 8 轮代码审查修复 60+ 线程安全/内存/协议问题
- [x] 压测（闭环 61,683 QPS / p99 461 μs；流水线长稳态 101 万 QPS；**单线程流水线 160 万 QPS**，8 连接 × 256 在途峰值 180 万）

## 项目结构
```text
rpc_framework/
├── CMakeLists.txt              # 构建配置
├── README.md                   # 项目说明
├── proto/
│   └── rpc_service.proto       # Protobuf 协议定义（8个message）
├── src/
│   ├── network/                # 网络层（epoll/Reactor）
│   │   ├── event_loop.h/cc     # Reactor 核心，epoll 封装
│   │   ├── event_loop_thread.h/cc   # one loop per thread
│   │   ├── event_loop_thread_pool.h/cc  # IO线程池轮询
│   │   ├── channel.h/cc        # fd + 事件回调封装
│   │   ├── buffer.h/cc         # 网络缓冲区（自动扩容）
│   │   ├── socket.h/cc         # socket 封装（新增静态工具方法）
│   │   ├── acceptor.h/cc       # 监听新连接
│   │   ├── connector.h/cc      # 非阻塞 connect + 指数退避重连（7.13 新增）
│   │   ├── tcp_connection.h/cc # 连接管理（状态机）
│   │   ├── tcp_server.h/cc     # 服务端（Acceptor + ThreadPool）
│   │   └── tcp_client.h/cc     # 客户端（Connector + TcpConnection，7.13 实现）
│   ├── codec/                  # 编解码器
│   │   └── rpc_codec.h/cc      # 统一协议：Fixed Header(18B) + Protobuf + CRC32
│   ├── client/                 # RPC 客户端
│   │   ├── rpc_sync_client.h/cc    # 同步阻塞调用 + 超时
│   │   ├── rpc_async_client.h/cc   # 异步 Future/Callback（基于 TcpClient，连接池/多节点）
│   │   └── inline_sync_client.h/cc # 同步调用：调用线程自己驱动 IO（无 eventfd/futex）
│   ├── server/                 # RPC 服务端
│   │   ├── rpc_service.h/cc    # 服务基类 + 方法注册
│   │   └── rpc_server.h/cc     # 服务注册 + 请求分发
│   ├── loadbalance/            # 负载均衡
│   │   └── consistent_hash.h/cc    # 一致性哈希 + 虚拟节点
│   ├── pool/                   # 连接池
│   │   └── connection_pool.h/cc    # 固定大小 + 健康检查
│   ├── circuit_breaker/        # 熔断器
│   │   └── circuit_breaker.h/cc    # 三态状态机
│   ├── rate_limiter/           # 限流
│   │   ├── token_bucket.h/cc       # 本地令牌桶
│   │   └── redis_token_bucket.h/cc # Redis 分布式（需 -DWITH_REDIS=ON）
│   ├── interceptor/             # 拦截器链
│   │   ├── interceptor.h/cc         # 责任链接口
│   │   └── builtin_interceptors.h/cc # 熔断/限流/Trace/Log 内置拦截器
│   └── discovery/              # 服务发现
│       ├── service_registry.h       # 抽象接口
│       ├── memory_registry.h/cc     # 内存实现
│       └── etcd_registry.h/cc       # etcd 实现（需 -DWITH_ETCD=ON）
├── examples/
│   ├── echo/                   # 裸TCP echo测试（旧协议，已废弃）
│   │   ├── echo_server.cc
│   │   └── echo_client.cc
│   └── rpc/                    # RPC 框架测试
│       ├── rpc_server_main.cc  # RpcServer + EchoService
│       ├── echo_service.h/cc   # Echo服务实现
│       ├── rpc_sync_test.cc    # 同步客户端测试
│       └── rpc_async_test.cc   # 异步客户端测试
├── tests/
│   ├── test_consistent_hash.cc     # 一致性哈希分布
│   ├── test_consistent_hash_lb.cc  # CH 负载均衡集成
│   ├── test_connector.cc           # Connector 连接/重连/析构
│   ├── test_memory_registry.cc     # 内存注册中心
│   ├── test_service_discovery_e2e.cc  # 服务发现端到端
│   ├── test_connection_pool.cc     # 连接池
│   ├── test_pool_ownership.cc      # TcpClient 所有权（共享连接必须显式失败）
│   ├── test_load_balance.cc        # 轮询负载均衡+failover
│   ├── test_circuit_breaker.cc     # 熔断器状态机
│   ├── test_token_bucket.cc        # 本地令牌桶
│   ├── test_redis_token_bucket.cc  # Redis 分布式令牌桶
│   ├── test_etcd_registry.cc       # etcd 注册/发现/Watch
│   ├── test_trace_id.cc            # TraceID 透传
│   ├── test_interceptor.cc         # 拦截器链
│   ├── test_integration.cc         # 全模块串联
│   ├── test_benchmark.cc           # 闭环压测（线程数 = 在途数）
│   ├── bench_pipeline.cc           # 流水线压测（在途数 >> 线程数，测吞吐上限）
│   └── bench_inline_sync.cc        # 调用线程驱动 IO 的同步压测
└── build/                          # 编译输出
```

## 协议设计

### 统一二进制协议格式
```text
┌─────────────────┬─────────────────┬─────────────────┐
│  Fixed Header   │  Variable Body  │    Checksum     │
│     18 Bytes    │   body_len B    │     4 Bytes     │
└─────────────────┴─────────────────┴─────────────────┘
```

### Fixed Header（18B）

| 字段 | 类型 | 大小 | 说明 |
|------|------|------|------|
| magic | uint32_t | 4B | 魔数 "RPCF" = 0x52504346 |
| version | uint8_t | 1B | 协议版本 = 1 |
| msg_type | uint8_t | 1B | 0=REQUEST, 1=RESPONSE, 2=HEARTBEAT |
| body_len | uint32_t | 4B | 变长体长度（网络字节序，解决粘包） |
| req_id | uint64_t | 8B | 请求ID |

> **修正**：7.12 协议统一化重构后，header 从 16B 调整为 **18B**（body_len 从 uint16_t 扩为 uint32_t）。

### 消息类型

- **Request**：service_name + method_name + protobuf payload
- **Response**：status + protobuf payload（含 success/payload/error_msg）
- **Heartbeat**：service_name + node_id + timestamp + extras

### 设计要点

1. **自描述**：header 中的 `body_len` 直接解决 TCP 粘包，无需外部 Length-Field 层
2. **统一性**：所有消息类型共用 FixedHeader，编解码逻辑统一
3. **可靠性**：CRC32 校验覆盖 header + body，防止数据损坏

## 核心数据流

### 服务端
```text
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  EchoService  │◀────│  RpcServer   │◀────│  Acceptor      │
│  (业务逻辑)   │     │  (方法分发)   │     │  (监听8888)    │
└─────────────┘     └─────────────┘     └─────────────┘
│
┌─────────────┐     ┌─────────────┐     ┌─────────────┘
│  RpcCodec    │◀────│ Fixed Header │◀────│  TcpConnection │
│  (编解码)    │     │ + Protobuf   │     │  (EventLoop)    │
└─────────────┘     │ + CRC32      │     │  (epoll LT)     │
└─────────────┘     └─────────────┘
```

### 客户端（7.13 重构后）
```text
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  RpcAsync    │────▶│  TcpClient   │────▶│  Connector    │────▶│  Socket      │
│  Client      │     │  (连接管理)   │     │  (非阻塞connect│     │  (fd)        │
│  (Future/CB) │◀────│              │◀────│  + 自动重连)  │     │              │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
│                                            │
└────────────┐                    ┌──────────┘
▼                    ▼
┌─────────────┐     ┌─────────────┐
│  TcpConnection│◀───│  连接成功后   │
│  (读写数据)   │     │  回调创建     │
└─────────────┘     └─────────────┘
```

## 拦截器链

```text
asyncCall()
  └─ chain_.execute(req)
       ├─ CircuitBreakerInterceptor::preHandle   → 拒绝"circuit breaker open"
       ├─ RateLimitInterceptor::preHandle         → 拒绝"rate limit exceeded"
       ├─ TraceInterceptor::preHandle             → 注入 trace-id
       ├─ LogInterceptor::preHandle               → 记录请求
       ├─ doCall() → encode → send → recv → return
       ├─ LogInterceptor::postHandle              → 记录响应
       ├─ TraceInterceptor::postHandle
       ├─ RateLimitInterceptor::postHandle
       └─ CircuitBreakerInterceptor::postHandle
```

preHandle 顺序执行（A→B→C），任一返回 false 立即终止；postHandle 逆序执行（C→B→A）。

## 网络层架构

### 服务端：Acceptor 模式
```text
MainReactor (Acceptor) ──accept()──► SubReactor Pool (EventLoopThreadPool)
│
▼
TcpConnection (读写)
```

### 客户端：Connector 模式（7.13 新增）
```text
RpcAsyncClient ──► TcpClient ──► Connector ──► Socket
│              │
│         指数退避重连
▼
TcpConnection (建立后)
```

## Connector 状态机
```text
                ┌─────────┐
     ┌─────────│  STOP   │◄────────┐
     │         │ (初始)   │         │
     │         └────┬────┘         │
     │              │ start()      │
     │              ▼              │
     │         ┌─────────┐         │
     │    ┌────│CONNECTING│◄───┐   │
     │    │    │(非阻塞connect)│   │   重连定时器触发
     │    │    └────┬────┘    │   │
     │    │         │         │   │
     │    │    可写/出错      │   │
     │    │         │         │   │
     │    │    ┌────┴────┐    │   │
     │    └───►│CONNECTED │    │   │
     │         │(连接成功) │    │   │
     │         └────┬────┘    │   │
     │              │ close()  │   │
     │              ▼          │   │
     │         ┌─────────┐     │   │
     └────────►│DISCONNECTED│───┘   │
               │(连接断开)  │────────┘
               └─────────┘
                       
     重连策略：500ms → 1s → 2s → 4s → 8s → max 30s
```

## 已完成 vs 待完成

| 模块 | 状态 |
|------|------|
| 网络层（epoll + Reactor + one loop per thread） | ✅ |
| 统一二进制协议（18B Header + Protobuf + CRC32） | ✅ |
| 同步/异步客户端（Future/Callback 双模式） | ✅ |
| Connector 非阻塞 connect + 指数退避重连 | ✅ |
| TcpClient 客户端底座 | ✅ |
| 服务端注册分发 | ✅ |
| 一致性哈希负载均衡 | ✅ |
| 轮询负载均衡（多节点 failover） | ✅ |
| 服务发现（MemoryRegistry + EtcdRegistry） | ✅ |
| 连接池（固定大小 + 健康检查） | ✅ |
| 熔断器（CLOSED→OPEN→HALF_OPEN） | ✅ |
| 限流（本地令牌桶 + Redis 双层） | ✅ |
| TraceID 透传 | ✅ |
| 拦截器链（责任链模式） | ✅ |
| 压测（闭环 61,683 QPS；流水线 101 万 QPS） | ✅ |

## 测试覆盖

| 测试 | 说明 |
|------|------|
| `test_connector` | Connector 连接成功 / 重连 / 析构安全 |
| `test_consistent_hash` | 一致性哈希分布 + 节点删除迁移 |
| `test_consistent_hash_lb` | 一致性哈希负载均衡集成 |
| `test_memory_registry` | 内存注册中心 CRUD |
| `test_service_discovery_e2e` | 服务发现端到端 |
| `test_connection_pool` | 连接池创建 / acquire / 轮询分布 |
| `test_load_balance` | 多节点轮询 + failover |
| `test_circuit_breaker` | 熔断器状态机 + RPC 集成 |
| `test_token_bucket` | 本地令牌桶 + 动态速率 |
| `test_redis_token_bucket` | Redis 分布式限流 + 降级（需 `-DWITH_REDIS=ON`） |
| `test_etcd_registry` | etcd 注册/发现/Watch（需 `-DWITH_ETCD=ON`） |
| `test_trace_id` | TraceID 注入 + 提取 |
| `test_interceptor` | 拦截器链顺序 + 阻塞 + RPC 集成 |
| `test_integration` | 全模块串联（LB + 熔断 + 限流 + 故障恢复） |
| `test_pool_ownership` | TcpClient 所有权：共享连接必须显式失败，不能串包 |
| `test_benchmark` | 压测（可指定请求数和线程数） |

## 踩坑记录

### 1. 同步客户端两套 socket 代码难以维护
**问题：** 最初同步客户端用阻塞 socket + select，异步客户端用 epoll + EventLoop，网络层两套代码。  
**解决：** 同步客户端底层复用 `RpcAsyncClient`，通过 `std::future::wait_for` 实现阻塞等待。网络层统一，面试能说清楚"同步是异步的语法糖"。

### 2. EventLoop 回调队列多线程竞争
**问题：** `queueInLoop` 把回调丢进 `pendingFunctors_`，但多个线程同时调用时没加锁，高并发必崩。  
**解决：** `pendingFunctors_` 加 `std::mutex`，`doPendingFunctors` 时 swap 出来减少锁持有时间。

### 3. ConsistentHash 多线程读写崩溃
**问题：** 服务发现更新节点列表和客户端并发选节点同时发生，map 被修改时遍历崩溃。  
**解决：** 全用 `std::mutex` 保护（C++14 兼容），读写都用 `lock_guard`。C++17 后可换 `shared_mutex` 优化读性能。

### 4. idle 超时连接断开 Segmentation Fault
**问题：** `TcpServer::removeConnectionInLoop` 里 `connections_.erase` 后 `conn` 引用计数降为 0，异步执行 `connectDestroyed` 时对象已析构。  
**解决：** `queueInLoop` 用 lambda 捕获 `shared_ptr` 延长生命周期，确保 `connectDestroyed` 执行时对象还在。

### 5. 连接关闭日志打印 "new connection"
**问题：** `onConnection` 用 `context_ == nullptr` 判断连接状态，但断开时 `context_` 已非空，导致日志反了。  
**解决：** 加 `TcpConnection::connected()` 方法，通过 `state_ == kConnected` 判断，逻辑清晰。

### 6. 协议不统一导致维护困难
**问题：** Request/Response/Heartbeat 三种消息结构不一致，外部又包了一层 Length-Field，编解码逻辑三套。  
**解决：** 统一为 Fixed Header(18B) + Variable Body + Checksum 的三段式结构，header 中的 `body_len` 自描述解决粘包，去掉冗余的 `encodeWithLength`/`decodeWithLength`。

### 7. 非阻塞 connect 结果判断
**问题：** `connect()` 返回 -1 且 `errno == EINPROGRESS` 时，需要通过 epoll 监听可写事件，再用 `getsockopt(SO_ERROR)` 获取真实错误码。  
**解决：** Connector 封装非阻塞 connect 全流程：socket() → connect() → epoll 监听可写 → SO_ERROR 判断 → 成功回调 / 失败重连。

### 8. 两个"看起来很对"的优化假设，被 A/B 实测证伪
**问题：** 压测到 5 万 QPS 后，直觉判断瓶颈是"服务端只有一个 Reactor"和"每请求一个超时定时器"，准备直接优化。  
**解决：** 先做单变量对照：服务端 IO 线程 1 → 8 只 +21%，关掉每请求定时器只 +1.4%，两个假设都错了。改用 `strace -c` 数系统调用，发现每请求 **14 次调用**（4 write / 4 futex / 3 epoll_wait / 2 readv / 1 read），其中 7 次纯粹是线程间搬运（4 futex + 2 次 eventfd 写 + 1 次 eventfd 读）；80,000 请求全程只用了约 1 个核。结论是**延迟瓶颈而非 CPU 瓶颈**，方向应改为"减少线程跳转 + 提高在途请求数"，而不是加线程。  
**教训：** 没有 A/B 对照的优化都是猜测；先量化，再动手。

### 9. 连接池把同一条连接交给两个客户端
**问题：** 线程数 > 连接数时 `acquire` 轮询返回同一个 `TcpClient`，两个 `RpcAsyncClient` 构造时互相覆盖 `TcpClient`/`TcpConnection` 的回调 → 响应被投递到错误的客户端 → `reqId` 匹配不到 → 一直等到超时。8 线程 / 4 连接实测 **4 个错误、耗时卡在 3.00 s**。  
**解决：** 给 `TcpClient` 加所有权声明（CAS 抢占）：第二个客户端构造即失败、调用立即返回 `"TcpClient already owned by another RpcAsyncClient"`；`acquire` 跳过已占用连接，全部占用时返回 `nullptr`；补 `test_pool_ownership` 回归测试。修复后同场景 **0 错误、0.84 s、59,852 QPS**。

### 10. 三层 use-after-free：回调栈上把对象销毁了
**问题：** `test_connection_pool` 偶发 segfault（跑十几轮才出现一次）。用 ASan 一跑就现形，而且是**三个独立的生命周期 bug**，全都发生在"回调里销毁了正在被分发的对象"：
1. `TcpConnection::handleClose` 在 `Channel::handleEvent` 的栈上析构了连接（连同 Channel），返回后继续读 `revents_`；
2. `Connector::handleWrite/handleError` 在 `Channel::handleEvent` 的栈上 `reset()` 了自己的 Channel；
3. `TcpClient` 在主线程析构后，IO 线程仍通过连接的 `closeCallback_`（裸 `this`）回调它；`Connector::start/stop` 同理。

**解决：**
1. `Channel` 增加 `tie(weak_ptr<owner>)`：`handleEvent` 开头 `lock()` 成 `shared_ptr`，让 owner 在整个分发期存活，析构自然推迟到函数返回后；
2. `Connector::removeAndResetChannel` 不再同步析构，改为把 Channel 交给本轮 loop 末尾的 pending functor 持有；
3. `closeCallback_` 与 `Connector::start/stop` 的投递全部改用 `weak_ptr`，对象没了就跳过。

**验证：** ASan 下 `test_connection_pool` **20/20 通过**（修复前 1/1 必现），全量 13 项测试通过，闭环/inline 基准无回退。

## 快速开始

```bash
git clone https://github.com/FSSLI/rpc_framework.git
cd rpc_framework
mkdir build && cd build
cmake .. && make -j$(nproc)

# 运行全部测试
./test_connector && ./test_connection_pool && ./test_load_balance && \
./test_consistent_hash_lb && ./test_circuit_breaker && ./test_token_bucket && \
./test_trace_id && ./test_interceptor && ./test_integration

# 压测（请求数 / 客户端线程数 / 服务端从Reactor数 / 超时ms；only = 只跑扩展性测试）
./test_benchmark 50000 8
./test_benchmark 50000 8 4 3000 only

# 可选：Redis 令牌桶
docker run -d --name redis-test -p 6379:6379 redis:7-alpine
cmake .. -DWITH_REDIS=ON && make -j$(nproc) && ./test_redis_token_bucket

# 可选：etcd 服务发现
docker run -d --name etcd-test -p 2379:2379 bitnami/etcd:3.5
cmake .. -DWITH_ETCD=ON && make -j$(nproc) && ./test_etcd_registry
```

## 性能基准

测试环境：WSL2（Ubuntu 22.04，i7-13620H，16 vCPU）/ 同机回环 / Release 构建 / `-DRPC_SILENT=ON` 关闭日志干扰
服务端配置：主 Reactor + **4 个从 Reactor**（`setThreadNum(4)`，见 `examples/rpc/rpc_server_main.cc`）

> **口径说明（重要）**：压测客户端是"闭环同步"模型——每个工作线程发一个请求、`future.get()` 等到响应再发下一个，因此 `QPS = 在途请求数 / RTT`。
> 所以下表是**延迟口径下的吞吐**，不是框架上限；真实上限要用流水线（单连接多请求在途）才测得出来。
> 完整实测过程与瓶颈定位见 [`docs/bench_results_wsl.md`](docs/bench_results_wsl.md)。

### Direct（1 条连接，N 个客户端工作线程）

| 线程 | QPS | avg (μs) | p99 (μs) | 错误数 |
|------|------|----------|----------|--------|
| 4 | 40,154 | 97 | 234 | 0 |
| 8 | 41,051 | 188 | 724 | 0 |

单连接吞吐稳定在 **~41k**，与客户端线程数无关——瓶颈是"单连接 + 单个客户端 EventLoop 线程"。

### Pool（连接池，N 线程 × N 连接，50000 req）

| 线程 | 连接数 | QPS | avg (μs) | p99 (μs) | 相对 1 线程 |
|------|--------|------|----------|----------|-------------|
| 1 | 1 | 14,297 | 70 | 200 | 1.00× |
| 2 | 2 | 23,272 | 86 | 395 | 1.63× |
| 4 | 4 | 37,126 | 106 | 394 | 2.60× |
| 8 | 8 | **61,683** | **128** | **461** | **4.31×** |

### 流水线（Pipelined，在途请求数 >> 线程数）

> 上面测的是**延迟**；这里用回调 API 让每条连接维持 N 个在途请求，压出**真实吞吐上限**。
> 注意：延迟包含排队（Little's law），**p99 不要和闭环那组直接对比**。

| 连接数 | 每连接在途 | 总在途 | QPS | avg (μs) | p99 (μs) |
|---|---|---|---|---|---|
| 1 | 1 | 1 | 14,841 | 51 | 169 |
| 1 | 32 | 32 | 205,431 | 113 | 882 |
| 1 | 512 | 512 | 345,033 | 907 | 3,076 |
| 8 | 64 | 512 | 1,111,995 | 332 | 1,907 |
| 8 | 64（**1000 万请求**） | 512 | **1,013,298** | 342 | 2,197 |
| 24 | 64 | 1,536 | **1,382,747** | 881 | 4,578 |

**同一个框架：闭环 61,683 → 流水线长稳态 101 万（16.4×），峰值 138 万（22×）。** 6 万不是吞吐上限，而是"在途请求数 = 线程数"这个测量方式的上限。
注意**短跑会高估**：200 万请求跑出 116.8 万，1000 万请求稳态只有 101 万，对外报 101 万。
流水线下服务端并行度重新成为瓶颈：从 Reactor 1 → 8，QPS 从 60.4 万涨到 113.4 万。

#### 进阶：单线程流水线（`InlinePipelineClient`）

上面的数字用 `RpcAsyncClient`（每连接 2 个线程：提交 + IO，每请求 1 次 eventfd）。
把"调用线程驱动 IO"和"流水线"叠起来后（`src/client/inline_pipeline_client.*`：
1 个线程自己发自己收、私有 pending 表无锁、无每请求定时器、无 eventfd）：

| 版本（8 连接 × 64 在途，200 万请求） | QPS | avg | p99 | CPU | 每请求 CPU |
|---|---|---|---|---|---|
| 跨线程流水线 `bench_pipeline` | 115~123 万 | 292~309 µs | 1.5~1.6 ms | 8.8~9.2 核 | ~10.3 µs |
| **单线程流水线 `bench_inline_pipeline`** | **139~156 万** | 281~312 µs | 1.0~1.5 ms | **7.3~7.8 核** | **~6.0 µs** |

| 配置 | QPS |
|---|---|
| 8 连接 × 256 在途 | **180 万** |
| **16 连接 × 64 在途，1000 万请求（长稳态）** | **160 万**（CPU 8.4 核，avg 601 µs） |

**结论：少一半线程的损失 < 少一半 syscall 的收益。** 160 万 QPS 只用 8.4/16 核，仍有余量。

### 瓶颈定位（A/B 实测，不是推测）

| 观察 | 数据 | 结论 |
|------|------|------|
| 服务端 IO 线程 1 → 8 | QPS +21%（51,240 → 61,901） | 服务端并行度不是吞吐瓶颈 |
| 关闭每请求超时定时器 | QPS +1.4%（57,771 → 58,594） | 定时器不是主要开销 |
| 每请求系统调用 | **14.15 次**（4 write / 4 futex / 3 epoll_wait / 2 readv / 1 read） | 其中 7 次是线程间搬运（4 futex + 2 eventfd 写 + 1 eventfd 读） |
| CPU 占用 | 80,000 请求共 7.49s，16 核只用了约 1 核 | 不是 CPU 瓶颈，是延迟瓶颈 |

优化方向：把同步调用改成"在调用线程里跑 loop"（省掉 2 次 eventfd 写 + 4 次 futex），并把 Future 做成真异步以支持流水线。

### 优化验证：同步调用改为"调用线程驱动 IO"

针对上面"14 次系统调用里有 7 次是线程间搬运"的结论，新增 `InlineSyncClient`（`src/client/inline_sync_client.*`）：
`EventLoop` 构造在调用线程，发送直接 `write()`，等待响应时在本线程 `loopOnce()`，全程无 eventfd / futex / 每请求定时器。

| 版本（8 线程 × 8 连接，5 万请求） | 系统调用/请求 | QPS | avg (µs) | p99 (µs) |
|---|---|---|---|---|
| 跨线程投递（`RpcAsyncClient` + `future.get()`） | 14.15 | 61,683 | 128 | 461 |
| **调用线程驱动（`InlineSyncClient`）** | **6.06** | **131,996** | **57** | **180** |

`futex` 从 4.03/请求降到 0.01，`write` 从 4.02 降到 2.01（只剩客户端与服务端各一次真实 socket 写）。
代价：客户端有线程亲和、一连接一线程、串行调用（详见 [`docs/bench_results_wsl.md`](docs/bench_results_wsl.md) 第 11 节）。

### 性能优化关键点
- **关闭日志输出**：`-DRPC_SILENT=ON` 是压测必选，默认构建下热路径 `cout` 会吃掉 30-50% 吞吐
- **连接池 vs Direct**：1 连接 41k → 8 连接 61.7k（**+50%**）
- **错误率**：0%（回环）。注意**连接数必须 ≥ 线程数**——否则多个 client 会共享同一条连接、回调被覆盖，表现为 4 个错误 + 3 秒超时
- **重现方法**：`cmake .. -DRPC_SILENT=ON -DCMAKE_BUILD_TYPE=Release && make -j$(nproc) test_benchmark && ./test_benchmark 50000 8`
- **参数**：`./test_benchmark <总请求数> <客户端线程数> <服务端从Reactor数> <超时ms> [only]`；`only` = 只跑扩展性测试
- **流水线**：`./bench_pipeline <连接数> <每连接在途数> <总请求数> <服务端从Reactor数>`，用回调 API 压真实上限（**10M 请求长稳态 101 万 QPS**，24 连接峰值 138 万，CPU 约 11.6/16 核）

## 作者

马超 - 西北大学计算机硕士