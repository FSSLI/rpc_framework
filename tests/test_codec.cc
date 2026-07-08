// tests/test_codec.cc

#include "codec/rpc_codec.h"
#include <iostream>
#include <cassert>

using namespace rpc;

void testEncodeDecodeRequest() {
    std::cout << "=== Test: Encode/Decode Request ===" << std::endl;
    
    // 1. 构建 EchoRequest 业务消息
    rpc::EchoRequest echoReq;
    echoReq.set_message("Hello, RPC!");
    
    // 2. 序列化为 payload，塞入 RpcRequest
    std::string payload;
    echoReq.SerializeToString(&payload);
    
    rpc::RpcRequest req;
    req.set_service_name("EchoService");
    req.set_method_name("Echo");
    req.set_payload(payload);
    (*req.mutable_metadata())["trace_id"] = "abc123";
    
    // 3. 编码
    uint64_t reqId = 42;
    std::string packet = Codec::encodeRequest(req, reqId);
    
    std::cout << "Encoded packet size: " << packet.size() << " bytes" << std::endl;
    
    // 4. 解码
    Buffer buf;
    buf.append(packet);
    
    DecodedPacket decoded;
    bool success = Codec::decode(buf, decoded);
    
    assert(success);
    assert(decoded.msg_type == MsgType::REQUEST);
    assert(decoded.req_id == reqId);
    
    // 5. 验证业务数据
    const rpc::RpcRequest& decodedReq = decoded.rpc_request;
    assert(decodedReq.service_name() == "EchoService");
    assert(decodedReq.method_name() == "Echo");
    
    // 6. 解析 payload 里的 EchoRequest
    rpc::EchoRequest decodedEcho;
    decodedEcho.ParseFromString(decodedReq.payload());
    assert(decodedEcho.message() == "Hello, RPC!");
    
    std::cout << "Service: " << decodedReq.service_name() << std::endl;
    std::cout << "Method: " << decodedReq.method_name() << std::endl;
    std::cout << "Echo message: " << decodedEcho.message() << std::endl;
    std::cout << "✓ Request test passed" << std::endl << std::endl;
}

void testEncodeDecodeResponse() {
    std::cout << "=== Test: Encode/Decode Response ===" << std::endl;
    
    // 1. 构建 EchoResponse
    rpc::EchoResponse echoResp;
    echoResp.set_message("Hello, RPC!");
    
    std::string payload;
    echoResp.SerializeToString(&payload);
    
    rpc::RpcResponse resp;
    resp.set_success(true);
    resp.set_payload(payload);
    
    // 2. 编码
    uint64_t reqId = 42;
    std::string packet = Codec::encodeResponse(resp, reqId, Status::SUCCESS);
    
    std::cout << "Encoded packet size: " << packet.size() << " bytes" << std::endl;
    
    // 3. 解码
    Buffer buf;
    buf.append(packet);
    
    DecodedPacket decoded;
    bool success = Codec::decode(buf, decoded);
    
    assert(success);
    assert(decoded.msg_type == MsgType::RESPONSE);
    assert(decoded.req_id == reqId);
    
    // 4. 验证
    const rpc::RpcResponse& decodedResp = decoded.rpc_response;
    assert(decodedResp.success() == true);
    
    rpc::EchoResponse decodedEcho;
    decodedEcho.ParseFromString(decodedResp.payload());
    assert(decodedEcho.message() == "Hello, RPC!");
    
    std::cout << "Success: " << decodedResp.success() << std::endl;
    std::cout << "Echo message: " << decodedEcho.message() << std::endl;
    std::cout << "✓ Response test passed" << std::endl << std::endl;
}

void testEncodeDecodeHeartbeat() {
    std::cout << "=== Test: Encode/Decode Heartbeat ===" << std::endl;
    
    // 1. 构建 Heartbeat
    rpc::Heartbeat hb;
    hb.set_service_name("EchoService");
    hb.set_node_id("node_001");
    hb.set_timestamp(1234567890);
    (*hb.mutable_extra())["load"] = "0.5";
    
    // 2. 编码
    std::string packet = Codec::encodeHeartbeat(hb);
    
    std::cout << "Encoded packet size: " << packet.size() << " bytes" << std::endl;
    
    // 3. 解码
    Buffer buf;
    buf.append(packet);
    
    DecodedPacket decoded;
    bool success = Codec::decode(buf, decoded);
    
    assert(success);
    assert(decoded.msg_type == MsgType::HEARTBEAT);
    
    // 4. 验证
    const rpc::Heartbeat& decodedHb = decoded.heartbeat;
    assert(decodedHb.service_name() == "EchoService");
    assert(decodedHb.node_id() == "node_001");
    assert(decodedHb.timestamp() == 1234567890);
    
    std::cout << "Service: " << decodedHb.service_name() << std::endl;
    std::cout << "Node ID: " << decodedHb.node_id() << std::endl;
    std::cout << "Timestamp: " << decodedHb.timestamp() << std::endl;
    std::cout << "✓ Heartbeat test passed" << std::endl << std::endl;
}

void testPartialPacket() {
    std::cout << "=== Test: Partial Packet (粘包/拆包) ===" << std::endl;
    
    // 1. 编码两个请求
    rpc::RpcRequest req1;
    req1.set_service_name("Service1");
    req1.set_method_name("Method1");
    req1.set_payload("payload1");
    
    rpc::RpcRequest req2;
    req2.set_service_name("Service2");
    req2.set_method_name("Method2");
    req2.set_payload("payload2");
    
    std::string packet1 = Codec::encodeRequest(req1, 1);
    std::string packet2 = Codec::encodeRequest(req2, 2);
    
    // 2. 模拟粘包：两个包连在一起
    std::string combined = packet1 + packet2;
    
    Buffer buf;
    buf.append(combined);
    
    // 3. 应该能解析出第一个包
    DecodedPacket decoded1;
    bool success1 = Codec::decode(buf, decoded1);
    assert(success1);
    assert(decoded1.req_id == 1);
    std::cout << "First packet decoded, req_id=" << decoded1.req_id << std::endl;
    
    // 4. 应该能解析出第二个包
    DecodedPacket decoded2;
    bool success2 = Codec::decode(buf, decoded2);
    assert(success2);
    assert(decoded2.req_id == 2);
    std::cout << "Second packet decoded, req_id=" << decoded2.req_id << std::endl;
    
    // 5. 没有更多数据
    DecodedPacket decoded3;
    bool success3 = Codec::decode(buf, decoded3);
    assert(!success3);
    std::cout << "No more packets (expected)" << std::endl;
    
    std::cout << "✓ Partial packet test passed" << std::endl << std::endl;
}

void testCorruptedPacket() {
    std::cout << "=== Test: Corrupted Packet (CRC check) ===" << std::endl;
    
    rpc::RpcRequest req;
    req.set_service_name("EchoService");
    req.set_method_name("Echo");
    req.set_payload("test");
    
    std::string packet = Codec::encodeRequest(req, 99);
    
    // 篡改最后一个字节
    packet.back() ^= 0xFF;
    
    Buffer buf;
    buf.append(packet);
    
    DecodedPacket decoded;
    bool success = Codec::decode(buf, decoded);
    
    assert(!success);  // CRC 校验失败，应该返回 false
    std::cout << "✓ Corrupted packet rejected (CRC check works)" << std::endl << std::endl;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "RPC Codec Test Suite" << std::endl;
    std::cout << "========================================" << std::endl << std::endl;
    
    testEncodeDecodeRequest();
    testEncodeDecodeResponse();
    testEncodeDecodeHeartbeat();
    testPartialPacket();
    testCorruptedPacket();
    
    std::cout << "========================================" << std::endl;
    std::cout << "All tests passed!" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return 0;
}