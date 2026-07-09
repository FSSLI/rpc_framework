# RPC Framework

基于 C++14 的高性能 RPC 框架，支持同步调用、服务注册发现、负载均衡、熔断降级。

## 特性

- [x] 自定义二进制协议（魔数 + 版本 + CRC32 校验）
- [x] Protobuf 序列化
- [x] TCP 粘包处理（Length-Field 帧格式）
- [x] epoll/Reactor 网络模型（主从 Reactor + one loop per thread）
- [x] 多 IO 线程（EventLoopThreadPool 轮询分发连接）
- [x] 同步 RPC 客户端（底层复用异步，future.wait_for 超时控制）
- [x] 异步 RPC 客户端（Future/Promise + Callback 双模式）
- [x] 服务端服务注册 + 方法分发（RpcServer）
- [x] 连接 idle 超时检测（服务端主动断开不活跃连接）
- [x] 一致性哈希负载均衡（MurmurHash + 虚拟节点）
- [ ] 服务注册发现（etcd）
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
│   │   ├── socket.h/cc         # socket 封装
│   │   ├── acceptor.h/cc       # 监听新连接
│   │   ├── tcp_connection.h/cc # 连接管理（状态机）
│   │   ├── tcp_server.h/cc     # 服务端（Acceptor + ThreadPool）
│   │   └── tcp_client.h        # 客户端骨架（待实现）
│   ├── codec/                  # 编解码器
│   │   ├── rpc_codec.h/cc      # Length-Field + Protobuf + CRC32
│   ├── client/                 # RPC 客户端
│   │   ├── rpc_sync_client.h/cc    # 同步阻塞调用 + 超时
│   │   └── rpc_async_client.h/cc   # 异步 Future/Callback
│   ├── server/                 # RPC 服务端
│   │   ├── rpc_service.h/cc    # 服务基类 + 方法注册
│   │   └── rpc_server.h/cc     # 服务注册 + 请求分发
│   ├── loadbalance/            # 负载均衡
│   │   ├── consistent_hash.h/cc    # 一致性哈希 + 虚拟节点
│   ├── common/                 # 公共工具（空）
│   └── discovery/              # 服务发现（空）
├── examples/
│   ├── echo/
│   │   ├── echo_server.cc      # 裸TCP echo测试
│   │   └── echo_client.cc      # 裸TCP echo客户端
│   └── rpc/
│       ├── rpc_server_main.cc  # RpcServer + EchoService
│       ├── echo_service.h/cc   # Echo服务实现
│       ├── rpc_sync_test.cc    # 同步客户端测试
│       └── rpc_async_test.cc   # 异步客户端测试
├── tests/
│   └── test_consistent_hash.cc # 一致性哈希测试
└── build/                      # 编译输出

## 核心数据流
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  RpcAsync    │────▶│  Length-Field │────▶│  TcpConnection │
│  Client      │     │  + Protobuf   │     │  (EventLoop)    │
│  (Future/CB) │◀────│  + CRC32     │◀────│  (epoll LT)     │
└─────────────┘     └─────────────┘     └─────────────┘
                                              │
┌─────────────┐     ┌─────────────┐     ┌─────────────┘
│  EchoService  │◀────│  RpcServer   │◀────│  Acceptor      │
│  (业务逻辑)   │     │  (方法分发)   │     │  (监听8888)    │
└─────────────┘     └─────────────┘     └─────────────┘

## 已完成 vs 待完成
模块	完成度	状态
网络层骨架	100%	✅
编解码器	100%	✅
同步/异步客户端	100%	✅
服务端注册分发	100%	✅
一致性哈希	100%	✅
服务发现（etcd）	0%	🔲 Week2
熔断降级	0%	🔲 Week2
限流（令牌桶）	0%	🔲 Week2
接入MySQL/Redis	0%	🔲 Week3
压测优化	0%	🔲 Week3

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
协议设计
详见 docs/protocol.md（待补充）
技术要点
网络模型：epoll LT + one loop per thread
协议格式：Length-Field + 固定头 + Protobuf Payload + CRC32
线程安全：std::future/promise 实现同步调用超时控制
作者
马超 - 西北大学计算机硕士