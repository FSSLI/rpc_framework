# RPC Framework

基于 C++14 的高性能 RPC 框架，支持同步调用、服务注册发现、负载均衡、熔断降级。

## 特性

- [x] 自定义二进制协议（魔数 + 版本 + CRC32 校验）
- [x] Protobuf 序列化
- [x] TCP 粘包处理（固定头 body_len 字段，自描述协议）
- [x] epoll/Reactor 网络模型（主从 Reactor + one loop per thread）
- [x] 多 IO 线程（EventLoopThreadPool 轮询分发连接）
- [x] 同步 RPC 客户端（底层复用异步，future.wait_for 超时控制）
- [x] 异步 RPC 客户端（Future/Promise + Callback 双模式，当前直接操作 socket，Week 2 重构为基于 TcpClient）
- [x] 服务端服务注册 + 方法分发（RpcServer）
- [x] 连接 idle 超时检测（服务端主动断开不活跃连接）
- [x] 一致性哈希负载均衡（MurmurHash + 虚拟节点）
- [ ] 服务注册发现（etcd）
- [ ] 熔断降级
- [ ] 限流（令牌桶）
- [ ] 接入 MySQL/Redis/MQ

## 项目结构

```
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
│   │   ├── socket.h/cc         # socket 封装
│   │   ├── acceptor.h/cc       # 监听新连接
│   │   ├── tcp_connection.h/cc # 连接管理（状态机）
│   │   ├── tcp_server.h/cc     # 服务端（Acceptor + ThreadPool）
│   │   └── tcp_client.h        # 客户端骨架（待实现，Week 2）
│   ├── codec/                  # 编解码器
│   │   └── rpc_codec.h/cc      # 统一协议：Fixed Header(16B) + Protobuf + CRC32
│   ├── client/                 # RPC 客户端
│   │   ├── rpc_sync_client.h/cc    # 同步阻塞调用 + 超时
│   │   └── rpc_async_client.h/cc   # 异步 Future/Callback（Week 2 重构）
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
│   └── test_consistent_hash.cc # 一致性哈希测试
└── build/                      # 编译输出
```

## 协议设计

### 统一二进制协议格式

```
┌─────────────────┬─────────────────┬─────────────────┐
│  Fixed Header   │  Variable Body  │    Checksum     │
│     16 Bytes    │   body_len B    │     4 Bytes     │
└─────────────────┴─────────────────┴─────────────────┘
```

### Fixed Header（16B）

| 字段 | 类型 | 大小 | 说明 |
|------|------|------|------|
| magic | uint32_t | 4B | 魔数 "RPCF" = 0x52504346 |
| version | uint8_t | 1B | 协议版本 = 1 |
| msg_type | uint8_t | 1B | 0=REQUEST, 1=RESPONSE, 2=HEARTBEAT |
| body_len | uint16_t | 2B | 变长体长度（网络字节序，解决粘包） |
| req_id | uint64_t | 8B | 请求ID |

### 消息类型

- **Request**：service_name + method_name + protobuf payload
- **Response**：status + protobuf payload + error_msg
- **Heartbeat**：service_name + node_id + timestamp + extras

### 设计要点

1. **自描述**：header 中的 `body_len` 直接解决 TCP 粘包，无需外部 Length-Field 层
2. **统一性**：所有消息类型共用 FixedHeader，编解码逻辑统一
3. **可靠性**：CRC32 校验覆盖 header + body，防止数据损坏

## 核心数据流

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  RpcAsync    │────▶│ Fixed Header │────▶│  TcpConnection │
│  Client      │     │ + Protobuf   │     │  (EventLoop)    │
│  (Future/CB) │◀────│ + CRC32      │◀────│  (epoll LT)     │
└─────────────┘     └─────────────┘     └─────────────┘
                                              │
┌─────────────┐     ┌─────────────┐     ┌─────────────┘
│  EchoService  │◀────│  RpcServer   │◀────│  Acceptor      │
│  (业务逻辑)   │     │  (方法分发)   │     │  (监听8888)    │
└─────────────┘     └─────────────┘     └─────────────┘
```

## 已完成 vs 待完成

| 模块 | 完成度 | 状态 |
|------|--------|------|
| 网络层骨架 | 100% | ✅ |
| 统一二进制协议 | 100% | ✅（7.11 重构，去掉 Length-Field） |
| 同步/异步客户端 | 90% | ✅（异步客户端 Week 2 重构为基于 TcpClient） |
| 服务端注册分发 | 100% | ✅ |
| 一致性哈希 | 100% | ✅ |
| 服务发现（etcd） | 0% | 🔲 Week 2 |
| 连接池 + 自动重连 | 0% | 🔲 Week 2 |
| 熔断降级 | 0% | 🔲 Week 2 |
| 限流（令牌桶） | 0% | 🔲 Week 2 |
| 接入 MySQL/Redis | 0% | 🔲 Week 3 |
| 压测优化 | 0% | 🔲 Week 3 |

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
**解决：** 统一为 Fixed Header(16B) + Variable Body + Checksum 的三段式结构，header 中的 `body_len` 自描述解决粘包，去掉冗余的 `encodeWithLength`/`decodeWithLength`。

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
```

## 技术要点

- **网络模型**：epoll LT + one loop per thread
- **协议格式**：Fixed Header(16B) + Protobuf Payload + CRC32（自描述，无需外部 Length-Field）
- **线程安全**：`std::future/promise` 实现同步调用超时控制
- **异步模式**：Future/Promise + Callback 双模式，req_id 关联请求-响应

## 作者

马超 - 西北大学计算机硕士
