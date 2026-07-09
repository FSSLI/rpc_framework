// examples/rpc/rpc_server_main.cc
#include "server/rpc_server.h"
#include "echo_service.h"
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

using namespace rpc;

int main() {
    EventLoop loop;
    
    struct sockaddr_in listenAddr;
    memset(&listenAddr, 0, sizeof(listenAddr));
    listenAddr.sin_family = AF_INET;
    listenAddr.sin_port = htons(8888);
    listenAddr.sin_addr.s_addr = INADDR_ANY;
    
    RpcServer server(&loop, listenAddr);
    
    // 注册 EchoService
    auto echoService = std::make_shared<EchoService>();
    server.registerService(echoService);
    
    server.start();
    
    std::cout << "RPC server listening on 8888" << std::endl;
    loop.loop();
    
    return 0;
}