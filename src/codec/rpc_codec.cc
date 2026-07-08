#include "codec/rpc_codec.h"

#include <arpa/inet.h>
#include <endian.h>
#include <cstring>

namespace rpc {

// ==================== CRC32 ====================

static const uint32_t kCrc32Table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba,
    0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
    0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
    0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de,
    0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec,
    0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
    0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
    0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940,
    0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116,
    0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
    0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
    0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a,
    0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818,
    0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
    0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
    0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c,
    0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2,
    0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
    0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
    0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086,
    0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4,
    0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
    0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
    0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8,
    0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe,
    0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
    0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
    0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252,
    0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60,
    0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
    0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
    0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04,
    0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a,
    0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
    0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
    0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e,
    0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c,
    0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
    0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
    0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0,
    0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6,
    0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
    0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

uint32_t Codec::crc32(const char* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = kCrc32Table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// ==================== 字节序转换 ====================

uint32_t Codec::encodeU32(uint32_t v) { return htonl(v); }
uint32_t Codec::decodeU32(uint32_t v) { return ntohl(v); }
uint64_t Codec::encodeU64(uint64_t v) { return htobe64(v); }
uint64_t Codec::decodeU64(uint64_t v) { return be64toh(v); }
uint16_t Codec::encodeU16(uint16_t v) { return htons(v); }
uint16_t Codec::decodeU16(uint16_t v) { return ntohs(v); }

// ==================== Buffer ====================

Buffer::Buffer() : readerIndex_(0), writerIndex_(0) {
    buffer_.resize(1024);
}

size_t Buffer::readableBytes() const { return writerIndex_ - readerIndex_; }
size_t Buffer::writableBytes() const { return buffer_.size() - writerIndex_; }

void Buffer::ensureWritableBytes(size_t len) {
    if (writableBytes() < len) {
        if (readerIndex_ + writableBytes() >= len) {
            size_t readable = readableBytes();
            std::memmove(buffer_.data(), buffer_.data() + readerIndex_, readable);
            readerIndex_ = 0;
            writerIndex_ = readable;
        } else {
            buffer_.resize(writerIndex_ + len);
        }
    }
}

void Buffer::append(const char* data, size_t len) {
    ensureWritableBytes(len);
    std::memcpy(buffer_.data() + writerIndex_, data, len);
    writerIndex_ += len;
}

void Buffer::append(const std::string& str) {
    append(str.data(), str.size());
}

const char* Buffer::peek() const {
    return buffer_.data() + readerIndex_;
}

std::string Buffer::retrieveAsString(size_t len) {
    std::string result(peek(), len);
    retrieve(len);
    return result;
}

std::string Buffer::retrieveAllAsString() {
    return retrieveAsString(readableBytes());
}

void Buffer::retrieve(size_t len) {
    if (len < readableBytes()) {
        readerIndex_ += len;
    } else {
        readerIndex_ = 0;
        writerIndex_ = 0;
    }
}

// ==================== 辅助函数 ====================

bool Codec::checkMagic(uint32_t magic) {
    return magic == kMagic;
}

void Codec::appendString(std::string& out, const std::string& str) {
    uint16_t len = encodeU16(static_cast<uint16_t>(str.size()));
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(str);
}

bool Codec::readString(const char* data, size_t& pos, size_t totalLen, std::string& out) {
    if (pos + sizeof(uint16_t) > totalLen) return false;
    uint16_t len = decodeU16(*reinterpret_cast<const uint16_t*>(data + pos));
    pos += sizeof(uint16_t);
    if (pos + len > totalLen) return false;
    out.assign(data + pos, len);
    pos += len;
    return true;
}

// ==================== 编码 ====================

std::string Codec::encodeRequest(const rpc::RpcRequest& req, uint64_t req_id) {
    std::string payload;
    if (!req.SerializeToString(&payload)) {
        return "";  // 序列化失败，返回空
    }
    
    std::string variablePart;
    appendString(variablePart, req.service_name());
    appendString(variablePart, req.method_name());
    
    uint32_t payloadLen = encodeU32(static_cast<uint32_t>(payload.size()));
    variablePart.append(reinterpret_cast<const char*>(&payloadLen), sizeof(payloadLen));
    variablePart.append(payload);
    
    RequestHeader header;
    header.magic = encodeU32(kMagic);
    header.version = kVersion;
    header.msg_type = static_cast<uint8_t>(MsgType::REQUEST);
    header.serialize_type = static_cast<uint8_t>(SerializeType::PROTOBUF);
    header.compress_type = static_cast<uint8_t>(CompressType::NONE);
    header.req_id = encodeU64(req_id);
    
    std::string packet;
    packet.append(reinterpret_cast<const char*>(&header), sizeof(header));
    packet.append(variablePart);
    
    uint32_t checksum = crc32(packet.data(), packet.size());
    uint32_t checksumBE = encodeU32(checksum);
    packet.append(reinterpret_cast<const char*>(&checksumBE), sizeof(checksumBE));
    
    return packet;
}

std::string Codec::encodeResponse(const rpc::RpcResponse& resp, 
                                 uint64_t req_id,
                                 Status status) {
    std::string payload;
    if (!resp.SerializeToString(&payload)) {
        return "";
    }
    
    ResponseHeader header;
    header.magic = encodeU32(kMagic);
    header.version = kVersion;
    header.msg_type = static_cast<uint8_t>(MsgType::RESPONSE);
    header.status = static_cast<uint8_t>(status);
    header.req_id = encodeU64(req_id);
    header.payload_len = encodeU32(static_cast<uint32_t>(payload.size()));
    
    std::string packet;
    packet.append(reinterpret_cast<const char*>(&header), sizeof(header));
    packet.append(payload);
    
    uint16_t errorLen = 0;
    packet.append(reinterpret_cast<const char*>(&errorLen), sizeof(errorLen));
    
    uint32_t checksum = crc32(packet.data(), packet.size());
    uint32_t checksumBE = encodeU32(checksum);
    packet.append(reinterpret_cast<const char*>(&checksumBE), sizeof(checksumBE));
    
    return packet;
}

std::string Codec::encodeHeartbeat(const rpc::Heartbeat& hb) {
    std::string packet;
    
    // 固定头
    HeartbeatHeader header;
    header.magic = encodeU32(kMagic);
    header.version = kVersion;
    header.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT);
    header.service_len = encodeU16(static_cast<uint16_t>(hb.service_name().size()));
    
    packet.append(reinterpret_cast<const char*>(&header), sizeof(header));
    
    // service
    packet.append(hb.service_name());
    
    // node_id
    appendString(packet, hb.node_id());
    
    // timestamp
    uint64_t timestamp = encodeU64(static_cast<uint64_t>(hb.timestamp()));
    packet.append(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
    
    // extra map
    uint16_t extraCount = encodeU16(static_cast<uint16_t>(hb.extra_size()));
    packet.append(reinterpret_cast<const char*>(&extraCount), sizeof(extraCount));
    for (const auto& pair : hb.extra()) {
        appendString(packet, pair.first);
        appendString(packet, pair.second);
    }
    
    // checksum
    uint32_t checksum = crc32(packet.data(), packet.size());
    uint32_t checksumBE = encodeU32(checksum);
    packet.append(reinterpret_cast<const char*>(&checksumBE), sizeof(checksumBE));
    
    return packet;
}

// ==================== 解码 ====================

bool Codec::decode(Buffer& buf, DecodedPacket& packet) {
    if (buf.readableBytes() < sizeof(uint32_t)) return false;
    
    uint32_t magic = decodeU32(*reinterpret_cast<const uint32_t*>(buf.peek()));
    if (!checkMagic(magic)) {
        buf.retrieve(1);
        return false;
    }
    
    if (buf.readableBytes() < 6) return false;
    uint8_t version = buf.peek()[4];
    uint8_t msgType = buf.peek()[5];
    
    if (version != kVersion) {
        // buf.retrieve(6);
        return false;
    }
    
    switch (static_cast<MsgType>(msgType)) {
        case MsgType::REQUEST:
            return decodeRequest(buf, packet);
        case MsgType::RESPONSE:
            return decodeResponse(buf, packet);
        case MsgType::HEARTBEAT:
            return decodeHeartbeat(buf, packet);
        default:
            buf.retrieve(4);
            return false;
    }
}

bool Codec::decodeRequest(Buffer& buf, DecodedPacket& packet) {
    if (buf.readableBytes() < kRequestHeaderSize + sizeof(uint16_t)) return false;
    
    const char* data = buf.peek();
    RequestHeader header;
    std::memcpy(&header, data, sizeof(header));
    
    header.magic = decodeU32(header.magic);
    header.req_id = decodeU64(header.req_id);
    
    size_t pos = kRequestHeaderSize;
    size_t totalLen = buf.readableBytes();
    
    std::string serviceName, methodName, payload;
    if (!readString(data, pos, totalLen, serviceName)) return false;
    if (!readString(data, pos, totalLen, methodName)) return false;
    
    if (pos + sizeof(uint32_t) > totalLen) return false;
    uint32_t payloadLen = decodeU32(*reinterpret_cast<const uint32_t*>(data + pos));
    pos += sizeof(uint32_t);
    if (pos + payloadLen > totalLen) return false;
    payload.assign(data + pos, payloadLen);
    pos += payloadLen;
    
    if (pos + sizeof(uint32_t) > totalLen) return false;
    uint32_t checksum = decodeU32(*reinterpret_cast<const uint32_t*>(data + pos));
    pos += sizeof(uint32_t);
    
    uint32_t calcCrc = crc32(data, pos - sizeof(uint32_t));
    if (calcCrc != checksum) {
        buf.retrieve(pos);
        return false;
    }
    
    rpc::RpcRequest req;
    if (!req.ParseFromString(payload)) {
        buf.retrieve(pos);
        return false;
    }
    
    packet.msg_type = MsgType::REQUEST;
    packet.req_id = header.req_id;
    packet.rpc_request = req;
    
    buf.retrieve(pos);
    return true;
}

bool Codec::decodeResponse(Buffer& buf, DecodedPacket& packet) {
    if (buf.readableBytes() < kResponseHeaderSize + sizeof(uint32_t)) return false;
    
    const char* data = buf.peek();
    ResponseHeader header;
    std::memcpy(&header, data, sizeof(header));
    
    header.magic = decodeU32(header.magic);
    header.req_id = decodeU64(header.req_id);
    header.payload_len = decodeU32(header.payload_len);
    
    size_t pos = kResponseHeaderSize;
    size_t totalLen = buf.readableBytes();
    
    if (pos + header.payload_len > totalLen) return false;
    std::string payload(data + pos, header.payload_len);
    pos += header.payload_len;
    
    if (pos + sizeof(uint16_t) > totalLen) return false;
    uint16_t errorLen = decodeU16(*reinterpret_cast<const uint16_t*>(data + pos));
    pos += sizeof(uint16_t);
    if (errorLen > 0) {
        if (pos + errorLen > totalLen) return false;
        pos += errorLen;
    }
    
    if (pos + sizeof(uint32_t) > totalLen) return false;
    uint32_t checksum = decodeU32(*reinterpret_cast<const uint32_t*>(data + pos));
    pos += sizeof(uint32_t);
    
    uint32_t calcCrc = crc32(data, pos - sizeof(uint32_t));
    if (calcCrc != checksum) {
        buf.retrieve(pos);
        return false;
    }
    
    rpc::RpcResponse resp;
    if (!resp.ParseFromString(payload)) {
        buf.retrieve(pos);
        return false;
    }
    
    packet.msg_type = MsgType::RESPONSE;
    packet.req_id = header.req_id;
    packet.rpc_response = resp;
    
    buf.retrieve(pos);
    return true;
}

bool Codec::decodeHeartbeat(Buffer& buf, DecodedPacket& packet) {
    // 最小：header(10B) + checksum(4B) = 14B
    // 但实际上必须有 service（至少0字节）+ node_id_len(2B) + timestamp(8B) + extra_count(2B)
    if (buf.readableBytes() < sizeof(HeartbeatHeader) + sizeof(uint32_t)) return false;
    
    const char* data = buf.peek();
    HeartbeatHeader header;
    std::memcpy(&header, data, sizeof(header));
    
    header.magic = decodeU32(header.magic);
    header.service_len = decodeU16(header.service_len);
    
    size_t pos = sizeof(HeartbeatHeader);
    size_t totalLen = buf.readableBytes();
    
    // service
    if (pos + header.service_len > totalLen) return false;
    std::string serviceName(data + pos, header.service_len);
    pos += header.service_len;
    
    // node_id
    std::string nodeId;
    if (!readString(data, pos, totalLen, nodeId)) return false;
    
    // timestamp
    if (pos + sizeof(uint64_t) > totalLen) return false;
    uint64_t timestamp = decodeU64(*reinterpret_cast<const uint64_t*>(data + pos));
    pos += sizeof(uint64_t);
    
    // extra map
    if (pos + sizeof(uint16_t) > totalLen) return false;
    uint16_t extraCount = decodeU16(*reinterpret_cast<const uint16_t*>(data + pos));
    pos += sizeof(uint16_t);
    
    rpc::Heartbeat hb;
    for (uint16_t i = 0; i < extraCount; ++i) {
        std::string key, value;
        if (!readString(data, pos, totalLen, key)) return false;
        if (!readString(data, pos, totalLen, value)) return false;
        (*hb.mutable_extra())[key] = value;
    }
    
    // checksum
    if (pos + sizeof(uint32_t) > totalLen) return false;
    uint32_t checksum = decodeU32(*reinterpret_cast<const uint32_t*>(data + pos));
    pos += sizeof(uint32_t);
    
    uint32_t calcCrc = crc32(data, pos - sizeof(uint32_t));
    if (calcCrc != checksum) {
        buf.retrieve(pos);
        return false;
    }
    
    hb.set_service_name(serviceName);
    hb.set_node_id(nodeId);
    hb.set_timestamp(static_cast<int64_t>(timestamp));
    
    packet.msg_type = MsgType::HEARTBEAT;
    packet.req_id = 0;
    packet.heartbeat = hb;
    
    buf.retrieve(pos);
    return true;
}

} // namespace rpc