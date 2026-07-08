# RPC协议设计 v1.1

## 设计目标
- 支持同步/异步RPC调用
- 支持服务注册发现
- 低延迟、高吞吐
- 易于扩展和调试

## 请求包 Request

| 字段 | 长度 | 说明 |
|------|------|------|
| magic | 4B | 魔数 0x52504346 ("RPCF") |
| version | 1B | 协议版本，当前1 |
| msg_type | 1B | 0:请求 1:响应 2:心跳 |
| serialize | 1B | 0:protobuf 1:json |
| compress | 1B | 0:none 1:zlib |  压缩
| req_id | 8B | 请求唯一ID（用于异步回调） |
| service_len | 2B | 服务名长度 |
| service | N B | 服务名（如"EchoService"） |
| method_len | 2B | 方法名长度 |
| method | N B | 方法名（如"Echo"） |
| payload_len | 4B | 参数长度 |
| payload | N B | protobuf序列化数据 |
| checksum | 4B | CRC32校验 |

## 响应包 Response

| 字段 | 长度 | 说明 |
|------|------|------|
| magic | 4B | 同上 |
| version | 1B | 同上 |
| msg_type | 1B | 1:响应 |
| status | 1B | 0:成功 1:失败 2:超时 |
| req_id | 8B | 对应请求的ID |
| payload_len | 4B | 结果长度 |
| payload | N B | protobuf序列化结果 |
| error_len | 2B | 错误信息长度（0表示无错误） |
| error | N B | 错误信息（失败时填充） |
| checksum | 4B | CRC32校验 |

## 心跳包 Heartbeat

| 字段 | 长度 | 说明 |
|------|------|------|
| magic | 4B | 同上 |
| version | 1B | 同上 |
| msg_type | 1B | 2:心跳 |
| service_len | 2B | 服务名长度 |
| service | N B | 服务名 |
| node_id_len | 2B | 节点ID长度 |
| node_id | N B | 节点唯一标识 |
| timestamp | 8B | 发送时间戳（毫秒） |
| extra_count | 2B | 扩展字段数量 |
| extra | 重复N次 | key_len(2B) + key + value_len(2B) + value |
| checksum | 4B | CRC32校验 |

## 设计说明

### 魔数 0x52504346
- ASCII: "RPCF"
- 作用：快速识别协议，防止误解析

### 请求ID req_id
- 8字节，全局唯一
- 同步调用：等待对应req_id的响应
- 异步调用：回调时匹配req_id

### 校验算法
- CRC32（IEEE 802.3标准）
- 覆盖除checksum字段外的所有数据

### 扩展性
- version字段支持协议升级
- compress字段预留压缩扩展
- 未来可添加trace_id用于链路追踪