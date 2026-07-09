// examples/rpc/echo_service.h
#ifndef ECHO_SERVICE_H
#define ECHO_SERVICE_H

#include "server/rpc_service.h"

namespace rpc {

class EchoService : public RpcService {
public:
    EchoService();
    
    std::string serviceName() const override { return "EchoService"; }

private:
    void echo(const RpcRequest& request, RpcResponse* response);
};

} // namespace rpc

#endif