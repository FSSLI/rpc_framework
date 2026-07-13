# RPC Framework

基于 C++14 的高性能 RPC 框架，支持同步调用、服务注册发现、负载均衡、熔断降级。

## 特性

- [x] 自定义二进制协议（魔数 + 版本 + CRC32 校验）
- [x] Protobuf 序列化
- [x] TCP 粘包处理（固定头 body_len 字段，自描述协议）
- [x] epoll/Reactor 网络模型（主从 Reactor + one loop per thread）
- [x] 多 IO 线程（EventLoopThreadPool 轮询分发连接）
- [x] 同步 RPC 客户端（底层复用异步，future.wait_for 超时控制）
- [x] 异步 RPC 客户端（Future/Promise + Callback 双模式）
- [x] **非阻塞 Connector + 自动重连（指数退避）**
- [x] **TcpClient 客户端底座（基于 Connector）**
- [x] 服务端服务注册 + 方法分发（RpcServer）
- [x] 连接 idle 超时检测（服务端主动断开不活跃连接）
- [x] 一致性哈希负载均衡（MurmurHash + 虚拟节点）
- [ ] 服务注册发现（etcd）
- [ ] 连接池（基于 TcpClient）
- [ ] 熔断降级
- [ ] 限流（令牌桶）
- [ ] 接入 MySQL/Redis/MQ

## 项目结构
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
│   │   └── rpc_async_client.h/cc   # 异步 Future/Callback（Week 2 重构为基于 TcpClient）
│   ├── server/                 # RPC 服务端
│   │   ├── rpc_service.h/cc    # 服务基类 + 方法注册
│   │   └── rpc_server.h/cc     # 服务注册 + 请求分发
│   ├── loadbalance/            # 负载均衡
│   │   └── consistent_hash.h/cc    # 一致性哈希 + 虚拟节点
│   ├── common/                 # 公共工具（空，Week 3 TraceId/Metrics）
│   └── discovery/              # 服务发现（空，Week 2 etcd）
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
│   ├── test_consistent_hash.cc # 一致性哈希测试
│   └── test_connector.cc       # Connector 连接测试（7.13 新增）
└── build/                      # 编译输出
plain

## 协议设计

### 统一二进制协议格式
┌─────────────────┬─────────────────┬─────────────────┐
│  Fixed Header   │  Variable Body  │    Checksum     │
│     18 Bytes    │   body_len B    │     4 Bytes     │
└─────────────────┴─────────────────┴─────────────────┘
plain

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
plain

### 客户端（7.13 重构后）
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
plain

## 网络层架构

### 服务端：Acceptor 模式
MainReactor (Acceptor) ──accept()──► SubReactor Pool (EventLoopThreadPool)
│
▼
TcpConnection (读写)
plain

### 客户端：Connector 模式（7.13 新增）
RpcAsyncClient ──► TcpClient ──► Connector ──► Socket
│              │
│         指数退避重连
▼
TcpConnection (建立后)
plain

## Connector 状态机
plain
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
plain

## 已完成 vs 待完成

| 模块 | 完成度 | 状态 |
|------|--------|------|
| 网络层骨架 | 100% | ✅ |
| 统一二进制协议 | 100% | ✅（7.12 重构，header 18B） |
| 同步/异步客户端 | 90% | ✅（异步客户端 Week 2 重构为基于 TcpClient） |
| **Connector + 自动重连** | **100%** | **✅（7.13 新增）** |
| **TcpClient 底座** | **100%** | **✅（7.13 新增）** |
| 服务端注册分发 | 100% | ✅ |
| 一致性哈希 | 100% | ✅ |
| 服务发现（etcd） | 0% | 🔲 Week 2 |
| 连接池 | 0% | 🔲 Week 2 |
| 熔断降级 | 0% | 🔲 Week 2 |
| 限流（令牌桶） | 0% | 🔲 Week 2 |
| 接入 MySQL/Redis | 0% | 🔲 Week 3 |
| 压测优化 | 0% | 🔲 Week 3 |

## 测试覆盖

| 测试项 | 状态 | 说明 |
|--------|------|------|
| 同步 RPC 调用 | ✅ | `rpc_sync_test` |
| 异步 RPC 调用（Future） | ✅ | `rpc_async_test` |
| 异步 RPC 调用（Callback） | ✅ | `rpc_async_test` |
| 一致性哈希 | ✅ | `test_consistent_hash` |
| Connector 连接成功 | ✅ | `test_connector` Test 1 |
| Connector 自动重连 | 🔲 | 待补：需 `EventLoop::cancelTimer` 或 `shared_ptr` 管理 Connector |

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

## 快速开始

```bash
# 克隆
git clone https://github.com/FSSLI/rpc_framework.git
cd rpc_framework

# 编译
mkdir build && cd build
cmake ..
make

# 运行 RPC Server
./rpc_server

# 运行同步客户端测试
./rpc_sync_test

# 运行异步客户端测试
./rpc_async_test

# 运行 Connector 测试
./test_connector
技术要点
网络模型：epoll LT + one loop per thread
协议格式：Fixed Header(18B) + Protobuf Payload + CRC32（自描述，无需外部 Length-Field）
线程安全：std::future/promise 实现同步调用超时控制
异步模式：Future/Promise + Callback 双模式，req_id 关联请求-响应
连接管理：Connector 非阻塞 connect + 指数退避重连（500ms → 1s → 2s → 4s → max 30s）
作者
马超 - 西北大学计算机硕士