// examples/rpc/rpc_sync_test.cc
#include "client/rpc_sync_client.h"
#include "protocol/rpc_service.pb.h"
#include <iostream>

using namespace rpc;

int main() {
    RpcSyncClient client("127.0.0.1", 8888);

    if (!client.connect()) {
        std::cerr << "connect failed" << std::endl;
        return 1;
    }

    EchoRequest echoReq;
    echoReq.set_message("hello sync rpc");

    RpcRequest req;
    req.set_payload(echoReq.SerializeAsString());

    try {
        RpcResponse resp = client.call("EchoService", "Echo", req, 5000);
        
        if (resp.success()) {
            EchoResponse echoResp;
            if (echoResp.ParseFromString(resp.payload())) {
                std::cout << "response: " << echoResp.message() << std::endl;
            }
        } else {
            std::cerr << "rpc failed: " << resp.error_msg() << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << std::endl;
    }

    return 0;
}