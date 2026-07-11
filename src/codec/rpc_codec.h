#ifndef RPC_CODEC_H
#define RPC_CODEC_H

#include <cstdint>
#include <string>
#include "network/buffer.h"
#include "protocol/rpc_service.pb.h"

namespace rpc {

// ==================== 常量 ====================

constexpr uint32_t kMagic = 0x52504346;  // "RPCF"
constexpr uint8_t  kVersion = 1;
constexpr size_t   kFixedHeaderSize = 18;  // 4+1+1+4+8
constexpr size_t   kChecksumSize = 4;

// ==================== 枚举 ====================

enum class MsgType : uint8_t {
    REQUEST   = 0,
    RESPONSE  = 1,
    HEARTBEAT = 2,
};

enum class SerializeType : uint8_t {
    PROTOBUF = 0,
    JSON     = 1,
};

enum class CompressType : uint8_t {
    NONE = 0,
    ZLIB = 1,
};

enum class Status : uint8_t {
    SUCCESS = 0,
    FAILED  = 1,
    TIMEOUT = 2,
};

// ==================== 统一固定头（1字节对齐） ====================
// 所有消息类型共用此头，body_len 解决粘包，无需外部 Length-Field
//
// 协议格式：
//   [Fixed Header: 16B][Variable Body: body_len B][Checksum: 4B]
//
// 总包长度 = 16 + body_len + 4 = 20 + body_len
// ============================================================================

#pragma pack(push, 1)  //一字节对齐

struct FixedHeader {
    uint32_t magic;      // 4B  魔数 "RPCF"
    uint8_t  version;    // 1B  协议版本
    uint8_t  msg_type;   // 1B  消息类型
    uint32_t body_len;   // 4B  变长体长度（网络字节序）
    uint64_t req_id;     // 8B  请求ID
};

#pragma pack(pop)

// ==================== 解码结果 ====================

struct DecodedPacket {
    MsgType msg_type;
    uint64_t req_id;

    rpc::RpcRequest rpc_request;
    rpc::RpcResponse rpc_response;
    rpc::Heartbeat heartbeat;

    // 辅助字段（仅 REQUEST 有效）
    std::string service_name;
    std::string method_name;
};

// ==================== Codec ====================

class Codec {
public:
    // 编码：返回完整包（header + body + checksum），直接发送，无需再加长度头
    static std::string encodeRequest(const rpc::RpcRequest& req, uint64_t req_id,
                                     const std::string& service_name,
                                     const std::string& method_name);
    static std::string encodeResponse(const rpc::RpcResponse& resp, 
                                      uint64_t req_id,
                                      Status status = Status::SUCCESS);
    static std::string encodeHeartbeat(const rpc::Heartbeat& hb, uint64_t req_id = 0);

    // 解码：从 Buffer 中尝试解码一个完整包
    // 返回值：true = 解码成功，packet 填充；false = 数据不足或校验失败
    static bool decode(Buffer& buf, DecodedPacket& packet);

    // CRC32 校验
    static uint32_t crc32(const char* data, size_t len);

    // 字节序转换（网络字节序 <-> 主机字节序）
    static uint32_t encodeU32(uint32_t v);
    static uint32_t decodeU32(uint32_t v);
    static uint64_t encodeU64(uint64_t v);
    static uint64_t decodeU64(uint64_t v);
    static uint16_t encodeU16(uint16_t v);
    static uint16_t decodeU16(uint16_t v);

private:
    static bool checkMagic(uint32_t magic);

    // 变长字符串编码：2B_len + str_bytes
    static void appendString(std::string& out, const std::string& str);
    static bool readString(const char* data, size_t& pos, size_t totalLen, std::string& out);

    // 各类型 body 编解码
    static std::string encodeRequestBody(const rpc::RpcRequest& req,
                                         const std::string& service_name,
                                         const std::string& method_name);
    static std::string encodeResponseBody(const rpc::RpcResponse& resp, Status status);
    static std::string encodeHeartbeatBody(const rpc::Heartbeat& hb);

    static bool decodeRequestBody(const char* data, size_t bodyLen, DecodedPacket& packet);
    static bool decodeResponseBody(const char* data, size_t bodyLen, DecodedPacket& packet);
    static bool decodeHeartbeatBody(const char* data, size_t bodyLen, DecodedPacket& packet);

    // 组装完整包：header + body + checksum
    static std::string pack(MsgType msg_type, uint64_t req_id, const std::string& body);
};

} // namespace rpc

#endif