// tests/test_connector.cc 简化版
#include "network/connector.h"
#include "network/event_loop.h"
#include "network/tcp_server.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <cstring>

using namespace rpc;

std::atomic<int> g_newConnectionCount{0};

void onNewConnection(int sockfd) {
    std::cout << "Connector: new connection established, fd=" << sockfd << std::endl;
    ++g_newConnectionCount;
    ::close(sockfd);
}

void testConnectSuccess() {
    std::cout << "\n=== Test 1: Connect Success ===" << std::endl;
    
    std::thread serverThread([]() {
        EventLoop serverLoop;
        struct sockaddr_in listenAddr;
        memset(&listenAddr, 0, sizeof(listenAddr));
        listenAddr.sin_family = AF_INET;
        listenAddr.sin_port = htons(9999);
        listenAddr.sin_addr.s_addr = INADDR_ANY;
        
        TcpServer server(&serverLoop, listenAddr);
        server.start();
        std::cout << "Server listening on 9999" << std::endl;
        serverLoop.loop();
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    std::thread clientThread([]() {
        EventLoop clientLoop;
        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(9999);
        inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);
        
        Connector connector(&clientLoop, serverAddr);
        connector.setNewConnectionCallback(onNewConnection);
        connector.start();
        
        // 1秒后退出
        clientLoop.runAfter(1.0, [&clientLoop]() {
            clientLoop.quit();
        });
        
        clientLoop.loop();
    });
    
    std::this_thread::sleep_for(std::chrono::seconds(1));
    
    std::cout << "newConnectionCount=" << g_newConnectionCount.load() << std::endl;
    if (g_newConnectionCount.load() == 1) {
        std::cout << "✅ Connect success test passed!" << std::endl;
    }
    
    serverThread.detach();
    clientThread.detach();
}

int main() {
    std::cout << "=== Connector Test ===" << std::endl;
    testConnectSuccess();
    std::cout << "\n=== All Tests Completed ===" << std::endl;
    return 0;
}