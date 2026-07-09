// examples/rpc/echo_service.cc
#include "echo_service.h"
#include "protocol/rpc_service.pb.h"

namespace rpc {

EchoService::EchoService() {
    registerMethod("Echo", [this](const RpcRequest& req, RpcResponse* resp) {
        this->echo(req, resp);
    });
}

void EchoService::echo(const RpcRequest& request, RpcResponse* response) {
    EchoRequest echoReq;
    if (!echoReq.ParseFromString(request.payload())) {
        response->set_success(false);
        response->set_error_msg("parse EchoRequest failed");
        return;
    }
    
    EchoResponse echoResp;
    echoResp.set_message(echoReq.message());
    
    response->set_success(true);
    response->set_payload(echoResp.SerializeAsString());
}

} // namespace rpc