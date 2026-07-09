// examples/rpc/rpc_async_test.cc
#include "client/rpc_async_client.h"
#include "protocol/rpc_service.pb.h"
#include <iostream>
#include <thread>
#include <chrono>

using namespace rpc;

int main() {
    RpcAsyncClient client("127.0.0.1", 8888);
    
    if (!client.connect()) {
        std::cerr << "connect failed" << std::endl;
        return 1;
    }
    
    // 测试 asyncCall + future
    EchoRequest echoReq;
    echoReq.set_message("hello async future");
    
    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());
    
    auto future = client.asyncCall("EchoService", "Echo", req);
    
    std::cout << "async call sent, waiting for response..." << std::endl;
    
    // 可以做其他事情...
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // 等待结果
    auto resp = future.get();
    if (resp.success()) {
        EchoResponse echoResp;
        if (echoResp.ParseFromString(resp.payload())) {
            std::cout << "future response: " << echoResp.message() << std::endl;
        }
    }
    
    // 测试 asyncCall + callback
    client.asyncCall("EchoService", "Echo", req, [](const RpcResponse& response) {
        if (response.success()) {
            EchoResponse resp;
            if (resp.ParseFromString(response.payload())) {
                std::cout << "callback response: " << resp.message() << std::endl;
            }
        }
    });
    
    // 等待回调执行
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    return 0;
}