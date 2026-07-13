// tests/test_connector.cc
// Connector 完整测试：连接成功、重连、析构安全

#include "network/connector.h"
#include "network/event_loop.h"
#include "network/tcp_server.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

using namespace rpc;

std::atomic<int> g_newConnectionCount{0};
std::atomic<bool> g_test2Done{false};
std::atomic<bool> g_test3Done{false};

// ============================================================================
// 测试1：连接成功
// ============================================================================

void onNewConnection(int sockfd) {
    std::cout << "  [Callback] New connection established, fd=" << sockfd << std::endl;
    ++g_newConnectionCount;
    ::close(sockfd);
}

bool testConnectSuccess() {
    std::cout << "=== Test 1: Connect Success ===" << std::endl;
    g_newConnectionCount = 0;

    std::atomic<bool> serverReady{false};
    std::atomic<bool> serverShouldQuit{false};

    std::thread serverThread([&]() {
        EventLoop serverLoop;
        struct sockaddr_in listenAddr;
        memset(&listenAddr, 0, sizeof(listenAddr));
        listenAddr.sin_family = AF_INET;
        listenAddr.sin_port = htons(9999);
        listenAddr.sin_addr.s_addr = INADDR_ANY;

        TcpServer server(&serverLoop, listenAddr);
        server.start();
        std::cout << "  Server listening on 9999" << std::endl;
        serverReady = true;

        // 用定时器检查退出条件
        serverLoop.runEvery(0.1, [&]() {
            if (serverShouldQuit.load()) {
                serverLoop.quit();
            }
        });

        serverLoop.loop();
    });

    while (!serverReady.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::atomic<bool> clientDone{false};
    std::thread clientThread([&]() {
        EventLoop clientLoop;
        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(9999);
        inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);

        auto connector = std::make_shared<Connector>(&clientLoop, serverAddr);
        connector->setNewConnectionCallback(onNewConnection);
        connector->start();

        // 0.5 秒后退出事件循环
        clientLoop.runAfter(0.5, [&]() {
            clientLoop.quit();
        });

        clientLoop.loop();  // ← 修复：必须运行事件循环
        clientDone = true;
    });

    clientThread.join();

    // 优雅退出服务端
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    serverShouldQuit = true;
    serverThread.join();

    bool passed = (g_newConnectionCount.load() == 1);
    std::cout << "  newConnectionCount=" << g_newConnectionCount.load() << std::endl;
    std::cout << (passed ? "  ✅ Connect success test passed!" : "  ❌ Connect success test failed!") << std::endl;
    return passed;
}

// ============================================================================
// 测试2：连接失败 + 重连（服务端不存在）
// ============================================================================

bool testConnectRetry() {
    std::cout << "=== Test 2: Connect Retry (No Server) ===" << std::endl;
    g_test2Done = false;

    std::thread clientThread([]() {
        EventLoop clientLoop;
        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(9998);
        inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);

        auto connector = std::make_shared<Connector>(&clientLoop, serverAddr);
        connector->setNewConnectionCallback([](int sockfd) {
            ::close(sockfd);
        });
        connector->start();

        // 2 秒后停止 Connector
        clientLoop.runAfter(2.0, [&]() {
            connector->stop();
        });
        // 再等 0.2 秒让 stop 生效后退出
        clientLoop.runAfter(2.2, [&]() {
            clientLoop.quit();
        });

        clientLoop.loop();  // ← 修复：必须运行事件循环
        g_test2Done = true;
    });

    clientThread.join();

    std::cout << "  Connector stopped without crash" << std::endl;
    std::cout << "  ✅ Retry test passed (no crash)" << std::endl;
    return true;
}

// ============================================================================
// 测试3：Connector 提前析构，weak_ptr 防止 use-after-free
// ============================================================================

bool testWeakPtrSafety() {
    std::cout << "=== Test 3: Weak Ptr Safety (Destructor Safety) ===" << std::endl;
    std::cout << "  Creating Connector, starting connection to non-existent server..." << std::endl;

    std::thread clientThread([]() {
        EventLoop clientLoop;
        struct sockaddr_in serverAddr;
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(9997);
        inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);

        auto connector = std::make_shared<Connector>(&clientLoop, serverAddr);
        connector->setNewConnectionCallback([](int sockfd) {
            ::close(sockfd);
        });
        connector->start();

        // 800ms 后析构 Connector（retry 定时器 500ms 先触发一次，再设 1000ms 定时器）
        // weak_ptr 保证 1000ms 定时器回调安全跳过
        clientLoop.runAfter(0.8, [&]() {
            std::cout << "  Deleting Connector before retry timer fires..." << std::endl;
            connector.reset();
        });

        // 2.5 秒后退出
        clientLoop.runAfter(2.5, [&]() {
            clientLoop.quit();
        });

        clientLoop.loop();  // ← 修复：必须运行事件循环
        g_test3Done = true;
    });

    clientThread.join();

    std::cout << "  No crash after Connector destroyed - weak_ptr works!" << std::endl;
    std::cout << "  ✅ Weak ptr safety test passed!" << std::endl;
    return true;
}

// ============================================================================
// 主函数
// ============================================================================

int main() {
    std::cout << "=== Connector Comprehensive Test ===" << std::endl;

    bool allPassed = true;
    allPassed &= testConnectSuccess();
    allPassed &= testConnectRetry();
    allPassed &= testWeakPtrSafety();

    std::cout << "========================================" << std::endl;
    if (allPassed) {
        std::cout << "All tests passed!" << std::endl;
    } else {
        std::cout << "Some tests failed!" << std::endl;
        return 1;
    }
    std::cout << "========================================" << std::endl;

    return 0;
}
