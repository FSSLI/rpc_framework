#ifndef RPC_CODEC_H
#define RPC_CODEC_H

#include <cstdint>
#include <string>
#include <vector>
#include "network/buffer.h"        // ← 替换

#include "protocol/rpc_service.pb.h"

namespace rpc {

// ==================== 常量 ====================

constexpr uint32_t kMagic = 0x52504346;  // "RPCF"
constexpr uint8_t  kVersion = 1;

// 固定头长度
constexpr size_t kRequestHeaderSize = 16;   // 4+1+1+1+1+8
constexpr size_t kResponseHeaderSize = 19;  // 4+1+1+1+8+4

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

// ==================== 协议头（纯固定字段，1字节对齐） ====================

#pragma pack(push, 1)

struct RequestHeader {
    uint32_t magic;           // 4B
    uint8_t  version;         // 1B
    uint8_t  msg_type;        // 1B (REQUEST=0)
    uint8_t  serialize_type;  // 1B
    uint8_t  compress_type;   // 1B
    uint64_t req_id;          // 8B
};

struct ResponseHeader {
    uint32_t magic;           // 4B
    uint8_t  version;         // 1B
    uint8_t  msg_type;        // 1B (RESPONSE=1)
    uint8_t  status;          // 1B
    uint64_t req_id;          // 8B
    uint32_t payload_len;     // 4B
};

struct HeartbeatHeader {
    uint32_t magic;           // 4B
    uint8_t  version;         // 1B
    uint8_t  msg_type;        // 1B (HEARTBEAT=2)
    uint16_t service_len;     // 2B
    // 后面跟变长：service + node_id_len(2B) + node_id + timestamp(8B) + extra_count(2B) + extras + checksum(4B)
};

#pragma pack(pop)

// ==================== 解码结果 ====================

struct DecodedPacket {
    MsgType msg_type;
    uint64_t req_id;
    
    rpc::RpcRequest rpc_request;
    rpc::RpcResponse rpc_response;
    rpc::Heartbeat heartbeat;
};

// ==================== Codec ====================

class Codec {
public:
    static std::string encodeRequest(const rpc::RpcRequest& req, uint64_t req_id,
                               const std::string& service_name,
                               const std::string& method_name);
    static std::string encodeResponse(const rpc::RpcResponse& resp, 
                                       uint64_t req_id,
                                       Status status = Status::SUCCESS);
    static std::string encodeHeartbeat(const rpc::Heartbeat& hb);

    static bool decode(Buffer& buf, DecodedPacket& packet,
                  std::string* service_name = nullptr,
                  std::string* method_name = nullptr);

    static uint32_t crc32(const char* data, size_t len);
    
    static uint32_t encodeU32(uint32_t v);
    static uint32_t decodeU32(uint32_t v);
    static uint64_t encodeU64(uint64_t v);
    static uint64_t decodeU64(uint64_t v);
    static uint16_t encodeU16(uint16_t v);
    static uint16_t decodeU16(uint16_t v);

    static std::string encodeWithLength(const std::string& packet);
    static bool decodeWithLength(Buffer& buf, std::string& packet);

private:
    static bool checkMagic(uint32_t magic);
    static void appendString(std::string& out, const std::string& str);
    static bool readString(const char* data, size_t& pos, size_t totalLen, std::string& out);
    
    static bool decodeRequest(Buffer& buf, DecodedPacket& packet,
                           std::string* service_name = nullptr,
                           std::string* method_name = nullptr);
    static bool decodeResponse(Buffer& buf, DecodedPacket& packet);
    static bool decodeHeartbeat(Buffer& buf, DecodedPacket& packet);
};

} // namespace rpc

#endif