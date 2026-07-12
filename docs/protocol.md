# RPC协议设计 v1.1

## 设计目标
- 支持同步/异步RPC调用
- 支持服务注册发现
- 低延迟、高吞吐
- 易于扩展和调试

## 统一包格式
┌─────────────────┬─────────────────┬─────────────────┐
│  Fixed Header   │  Variable Body  │    Checksum     │
│     18 Bytes    │   body_len B    │     4 Bytes     │
└─────────────────┴─────────────────┴─────────────────┘

## Fixed Header（18B）

| 字段 | 类型 | 长度 | 说明 |
|------|------|------|------|
| magic | uint32_t | 4B | 魔数 "RPCF" = 0x52504346 |
| version | uint8_t | 1B | 协议版本 = 1 |
| msg_type | uint8_t | 1B | 0=REQUEST, 1=RESPONSE, 2=HEARTBEAT |
| body_len | uint32_t | 4B | 变长体长度（网络字节序，解决粘包） |
| req_id | uint64_t | 8B | 请求唯一ID |

## 消息类型

### Request Body

| 字段 | 长度 | 说明 |
|------|------|------|
| service_len | 2B | 服务名长度 |
| service | N B | 服务名（如"EchoService"） |
| method_len | 2B | 方法名长度 |
| method | N B | 方法名（如"Echo"） |
| payload_len | 4B | 参数长度 |
| payload | N B | protobuf序列化数据（RpcRequest） |

### Response Body

| 字段 | 长度 | 说明 |
|------|------|------|
| status | 1B | 0=SUCCESS, 1=FAILED, 2=TIMEOUT |
| payload_len | 4B | 结果长度 |
| payload | N B | protobuf序列化数据（RpcResponse，含success/payload/error_msg） |

### Heartbeat Body

| 字段 | 长度 | 说明 |
|------|------|------|
| service_len | 2B | 服务名长度 |
| service | N B | 服务名 |
| node_id_len | 2B | 节点ID长度 |
| node_id | N B | 节点唯一标识 |
| timestamp | 8B | 发送时间戳（毫秒） |
| extra_count | 2B | 扩展字段数量 |
| extra | 重复N次 | key_len(2B) + key + value_len(2B) + value |

## 设计说明

### 自描述协议
- Header 中的 `body_len` 直接解决 TCP 粘包
- 无需外部 Length-Field 层，编解码统一

### 魔数 0x52504346
- ASCII: "RPCF"
- 作用：快速识别协议，防止误解析，支持流式重新同步

### 请求ID req_id
- 8字节，全局唯一
- 同步调用：等待对应req_id的响应
- 异步调用：回调时匹配req_id

### 校验算法
- CRC32（IEEE 802.3标准）
- 覆盖 Header + Body，不包括 Checksum 自身

### 扩展性
- version字段支持协议升级
- msg_type 可扩展新消息类型
- Heartbeat 的 extra 支持自定义扩展字段
- 未来可添加trace_id用于链路追踪（Header扩展或Body扩展）