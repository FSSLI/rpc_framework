# RPC Framework

基于 C++14 的高性能 RPC 框架，支持同步调用、服务注册发现、负载均衡、熔断降级。

## 特性

- [x] 自定义二进制协议（魔数 + 版本 + CRC32 校验）
- [x] Protobuf 序列化
- [x] TCP 粘包处理（Length-Field 帧格式）
- [x] epoll/Reactor 网络模型（one loop per thread）
- [x] 同步 RPC 客户端（阻塞调用 + 超时控制）
- [x] 服务注册 + 方法分发（RpcServer）
- [ ] 异步 RPC 客户端（Future/Promise）
- [ ] 服务注册发现（etcd）
- [ ] 负载均衡（轮询/一致性哈希）
- [ ] 熔断降级、限流

## 项目结构
rpc_framework/
├── CMakeLists.txt      # 构建配置
├── proto/              # Protobuf 定义
│   └── rpc_service.proto
├── src/
│   ├── codec/          # 编解码器
│   │   ├── rpc_codec.h
│   │   └── rpc_codec.cc
│   ├── network/        # 网络层（epoll/Reactor）
│   │   ├── event_loop.h/cc
│   │   ├── event_loop_thread.h/cc
│   │   ├── event_loop_thread_pool.h/cc
│   │   ├── channel.h/cc
│   │   ├── buffer.h/cc
│   │   ├── socket.h/cc
│   │   ├── acceptor.h/cc
│   │   ├── tcp_connection.h/cc
│   │   └── tcp_server.h/cc
│   ├── client/         # RPC 客户端
│   │   ├── rpc_sync_client.h
│   │   └── rpc_sync_client.cc
│   └── server/         # RPC 服务端
│       ├── rpc_service.h/cc
│       └── rpc_server.h/cc
├── examples/           # 示例
│   ├── echo/
│   │   ├── echo_server.cc
│   │   └── echo_client.cc
│   └── rpc/
│       ├── rpc_server_main.cc
│       ├── echo_service.h/cc
│       └── rpc_sync_test.cc
└── build/              # 编译输出
plain

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