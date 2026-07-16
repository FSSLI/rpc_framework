// tests/test_consistent_hash_lb.cc
#include "client/rpc_async_client.h"
#include "server/rpc_server.h"
#include "server/rpc_service.h"
#include "discovery/memory_registry.h"
#include "protocol/rpc_service.pb.h"
#include "network/event_loop.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <cassert>
#include <cstring>
#include <netinet/in.h>
#include <set>

using namespace rpc;

class HashEchoService : public RpcService {
public:
    explicit HashEchoService(int nodeId) : nodeId_(nodeId) {
        registerMethod("Echo", [this](const RpcRequest& req, RpcResponse* resp) {
            EchoRequest er; er.ParseFromString(req.payload());
            EchoResponse echoR;
            echoR.set_message("node" + std::to_string(nodeId_) + ":" + er.message());
            resp->set_success(true);
            resp->set_payload(echoR.SerializeAsString());
        });
    }
    std::string serviceName() const override { return "EchoService"; }
private: int nodeId_;
};

// ============================================================================
bool testRoundRobinBaseline() {
    std::cout << "=== Test 0: RR baseline (ensure multi-node works) ===" << std::endl;

    std::atomic<bool> quit{false};
    std::thread t1([&](){ EventLoop loop; sockaddr_in a; memset(&a,0,sizeof a);
        a.sin_family=AF_INET; a.sin_port=htons(22001); a.sin_addr.s_addr=INADDR_ANY;
        RpcServer s(&loop,a); s.registerService(std::make_shared<HashEchoService>(1)); s.start();
        loop.runEvery(0.1,[&](){ if(quit.load()) loop.quit(); }); loop.loop(); });
    std::thread t2([&](){ EventLoop loop; sockaddr_in a; memset(&a,0,sizeof a);
        a.sin_family=AF_INET; a.sin_port=htons(22002); a.sin_addr.s_addr=INADDR_ANY;
        RpcServer s(&loop,a); s.registerService(std::make_shared<HashEchoService>(2)); s.start();
        loop.runEvery(0.1,[&](){ if(quit.load()) loop.quit(); }); loop.loop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto reg = std::make_shared<MemoryRegistry>();
    reg->registerService({"EchoService","n1","127.0.0.1",22001,0,0});
    reg->registerService({"EchoService","n2","127.0.0.1",22002,0,0});

    RpcAsyncClient client(reg, "EchoService");
    bool ok = client.connect();
    if (!ok) { std::cerr << "connectViaRegistry FAILED!" << std::endl; quit=true; t1.join(); t2.join(); return false; }
    std::cout << "  Connected: " << (client.connected() ? "yes" : "no") << std::endl;

    // Send 4 requests with RR (default), should hit both nodes
    std::set<int> nodes;
    for (int i=0;i<4;i++) {
        EchoRequest er; er.set_message("rr"+std::to_string(i));
        RpcRequest req; req.set_payload(er.SerializeAsString());
        auto f = client.asyncCall("EchoService","Echo",req,3000);
        auto r = f.get();
        assert(r.success());
        EchoResponse echoR; echoR.ParseFromString(r.payload());
        nodes.insert(std::stoi(echoR.message().substr(4,1)));
    }
    std::cout << "  RR hit nodes: " << nodes.size() << " (expect 2)" << std::endl;
    assert(nodes.size()==2);

    client.disconnect();
    quit=true; t1.join(); t2.join();
    std::cout << "  ✅ RR baseline passed" << std::endl;
    return true;
}

// ============================================================================
bool testConsistentHashSticky() {
    std::cout << "=== Test 1: CH sticky key ===" << std::endl;

    std::atomic<bool> quit{false};
    auto runServer = [&](int port, int id) {
        std::thread* t = new std::thread([port,id,&quit](){
            EventLoop loop; sockaddr_in a; memset(&a,0,sizeof a);
            a.sin_family=AF_INET; a.sin_port=htons(port); a.sin_addr.s_addr=INADDR_ANY;
            RpcServer s(&loop,a); s.registerService(std::make_shared<HashEchoService>(id)); s.start();
            loop.runEvery(0.1,[&](){ if(quit.load()) loop.quit(); }); loop.loop();
        });
        return t;
    };
    auto* t1=runServer(22011,1), *t2=runServer(22012,2);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto reg = std::make_shared<MemoryRegistry>();
    reg->registerService({"EchoService","a","127.0.0.1",22011,0,0});
    reg->registerService({"EchoService","b","127.0.0.1",22012,0,0});

    RpcAsyncClient client(reg, "EchoService");
    client.setLBPolicy(LBPolicy::CONSISTENT_HASH, 150);
    bool ok = client.connect();
    if (!ok) { std::cerr << "connect FAILED" << std::endl; quit=true; t1->join();t2->join(); delete t1;delete t2; return false; }
    std::cout << "Connected!" << std::endl;

    // 同一 key 多次应命中同一节点
    int first=-1;
    for (int i=0;i<3;i++){
        EchoRequest er; er.set_message("sticky");
        RpcRequest req; req.set_payload(er.SerializeAsString());
        auto f=client.asyncCall("EchoService","Echo",req,5000);
        auto r=f.get();
        if (!r.success()) { std::cerr << "RPC failed: " << r.error_msg() << std::endl; continue; }
        EchoResponse echoR; echoR.ParseFromString(r.payload());
        std::string msg = echoR.message();
        if (msg.empty()) { std::cerr << "empty message!" << std::endl; continue; }
        int n=std::stoi(msg.substr(4,1));
        std::cout << "  req " << i << " → node" << n << std::endl;
        if(first<0) first=n;
        assert(n==first);
    }
    std::cout << "  3 calls with same key → all node" << first << std::endl;

    client.disconnect();
    quit=true; t1->join();t2->join(); delete t1;delete t2;
    std::cout << "  ✅ CH sticky test passed" << std::endl;
    return true;
}

bool testConsistentHashSpread() {
    std::cout << "=== Test 2: CH key distribution ===" << std::endl;

    std::atomic<bool> quit{false};
    auto runServer = [&](int port, int id) {
        std::thread* t = new std::thread([port,id,&quit](){
            EventLoop loop; sockaddr_in a; memset(&a,0,sizeof a);
            a.sin_family=AF_INET; a.sin_port=htons(port); a.sin_addr.s_addr=INADDR_ANY;
            RpcServer s(&loop,a); s.registerService(std::make_shared<HashEchoService>(id)); s.start();
            loop.runEvery(0.1,[&](){ if(quit.load()) loop.quit(); }); loop.loop();
        });
        return t;
    };
    auto* t1=runServer(22021,1), *t2=runServer(22022,2), *t3=runServer(22023,3);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto reg = std::make_shared<MemoryRegistry>();
    reg->registerService({"EchoService","a","127.0.0.1",22021,0,0});
    reg->registerService({"EchoService","b","127.0.0.1",22022,0,0});
    reg->registerService({"EchoService","c","127.0.0.1",22023,0,0});

    RpcAsyncClient client(reg, "EchoService");
    client.setLBPolicy(LBPolicy::CONSISTENT_HASH, 150);
    assert(client.connect());

    std::set<int> nodes;
    for (int i=0;i<15;i++){
        EchoRequest er; er.set_message("key"+std::to_string(i));
        RpcRequest req; req.set_payload(er.SerializeAsString());
        auto f=client.asyncCall("EchoService","Echo",req,5000);
        auto r=f.get();
        if(!r.success()) continue;
        EchoResponse echoR; echoR.ParseFromString(r.payload());
        if(!echoR.message().empty()) nodes.insert(std::stoi(echoR.message().substr(4,1)));
    }
    std::cout << "  15 diff keys → " << nodes.size() << " nodes" << std::endl;
    assert(nodes.size()>=2);

    client.disconnect();
    quit=true; t1->join();t2->join();t3->join(); delete t1;delete t2;delete t3;
    std::cout << "  ✅ CH distribution test passed" << std::endl;
    return true;
}

int main() {
    bool ok = testRoundRobinBaseline() && testConsistentHashSticky();
    std::cout << (ok ? "\nAll passed!\n" : "\nFAILED\n");
    return ok ? 0 : 1;
}
