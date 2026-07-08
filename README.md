# RPC Framework

基于 C++ 的高性能 RPC 框架，支持同步/异步调用、服务注册发现、负载均衡、熔断降级。

## 特性
- [x] 自定义二进制协议（魔数 + CRC32 校验）
- [x] Protobuf 序列化
- [x] TCP 粘包处理
- [ ] epoll/Reactor 网络模型
- [ ] 同步/异步 RPC 客户端
- [ ] 服务注册发现（etcd）
- [ ] 负载均衡（轮询/一致性哈希）
- [ ] 熔断降级、限流

## 项目结构
rpc_framework/
├── docs/           # 文档
├── proto/          # Protobuf 定义
├── src/
│   ├── codec/      # 编解码器
│   ├── network/    # 网络层（epoll/Reactor）
│   ├── client/     # RPC 客户端
│   ├── server/     # RPC 服务端
│   ├── common/     # 公共工具
│   ├── discovery/  # 服务发现
│   └── loadbalance/# 负载均衡
├── tests/          # 单元测试
└── examples/       # 示例

## 快速开始

```bash
# 编译
mkdir build && cd build
cmake ..
make

# 运行测试
./test_codec
协议设计
详见 docs/protocol.md
作者
马超 - 西北大学计算机硕士
