# 网络层设计文档（Network Layer Design Doc）

> 版本：v1.0 | 日期：2026.7.12 | 作者：马超
> 项目：基于 Muduo 思想的 C++ RPC 框架网络层

---

## 目录

1. [整体架构](#1-整体架构)
2. [核心设计思想](#2-核心设计思想)
3. [类关系图](#3-类关系图)
4. [核心类详解](#4-核心类详解)
   - 4.1 Socket（RAII 封装）
   - 4.2 Buffer（应用层缓冲区）
   - 4.3 Channel（fd 事件分发器）
   - 4.4 EventLoop（事件循环核心）
   - 4.5 EventLoopThread（IO 线程封装）
   - 4.6 EventLoopThreadPool（线程池）
   - 4.7 Acceptor（监听新连接）
   - 4.8 TcpConnection（连接生命周期）
   - 4.9 TcpServer（主从 Reactor）
   - 4.10 TcpClient（客户端底座）
5. [线程模型](#5-线程模型)
6. [踩坑记录](#6-踩坑记录)
7. [待完善（Week 2）](#7-待完善week-2)

---

## 1. 整体架构

采用 **主从 Reactor + One Loop Per Thread** 模型：

```
┌─────────────────────────────────────────────────────────────┐
│                        TcpServer                             │
│  ┌─────────────┐    ┌─────────────────────────────────────┐   │
│  │  Acceptor   │───▶│      EventLoopThreadPool          │   │
│  │ (主 Reactor)│    │  ┌─────────┐ ┌─────────┐ ┌──────┐ │   │
│  │  listen fd  │    │  │IO Loop 0│ │IO Loop 1│ │ ...  │ │   │
│  │  监听新连接  │    │  │(sub)    │ │(sub)    │ │      │ │   │
│  └─────────────┘    │  └─────────┘ └─────────┘ └──────┘ │   │
│         │           └─────────────────────────────────────┘   │
│         │                      ▲                            │
│         └──────────────────────┘                            │
│                    轮询分发新连接                            │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
            ┌─────────────┐     ┌─────────────┐
            │TcpConnection│     │TcpConnection│
            │  conn-0     │     │  conn-1     │
            │ (ioLoop-0)  │     │ (ioLoop-1)  │
            └─────────────┘     └─────────────┘
```

**核心流程**：
1. **主 Reactor**（baseLoop）运行 Acceptor，监听端口，accept 新连接
2. 新连接通过 `getNextLoop()` 轮询分发到某个 **从 Reactor**（subLoop）
3. 每个 TcpConnection 绑定到一个 subLoop，该 Loop 负责该连接的所有 IO
4. **One Loop Per Thread**：每个 Loop 跑在一个独立线程，无锁处理读写

---

## 2. 核心设计思想

| 设计原则 | 实现方式 | 说明 |
|---------|---------|------|
| **RAII** | Socket、Channel、TcpConnection | 构造函数获取资源，析构函数释放，禁止拷贝 |
| **非阻塞 IO** | `SOCK_NONBLOCK` + `epoll` | 所有 fd 非阻塞，IO 多路复用 |
| **Reactor 模式** | EventLoop + Channel | 事件驱动，回调处理 |
| **One Loop Per Thread** | EventLoopThreadPool | 每个 IO 线程一个 EventLoop，避免锁竞争 |
| **跨线程安全** | `runInLoop` / `queueInLoop` | 非 Loop 线程投递任务，通过 `eventfd` 唤醒 |
| **生命周期管理** | `shared_ptr` + `enable_shared_from_this` | TcpConnection 多处引用，shared_ptr 自动管理 |

---

## 3. 类关系图

```
                        ┌─────────────┐
                        │   Buffer    │
                        │  (缓冲区)    │
                        └──────┬──────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
   ┌────┴────┐           ┌─────┴─────┐         ┌──────┴──────┐
   │  Socket │◄─────────│  Channel  │◄────────│  EventLoop  │
   │ (fd封装) │          │(事件分发)  │         │ (事件循环)  │
   └────┬────┘           └─────┬─────┘         └──────┬──────┘
        │                      │                      │
        │              ┌───────┴───────┐              │
        │              │               │              │
   ┌────┴────┐    ┌───┴───┐      ┌───┴───┐    ┌────┴────┐
   │ Acceptor │    │TcpConn│      │wakeup │    │EventLoop│
   │(监听fd)  │    │(连接fd)│     │Channel│    │ Thread  │
   └────┬────┘    └───┬───┘      └───────┘    └────┬────┘
        │             │                             │
        │             │                      ┌────┴────┐
        │             │                      │ThreadPool│
        │             │                      └────┬────┘
        │             │                           │
   ┌────┴─────────────┴───────────────────────────┴────┐
   │                  TcpServer                         │
   │              (连接管理 + 线程调度)                    │
   └────────────────────────────────────────────────────┘
```

---

## 4. 核心类详解

### 4.1 Socket —— fd 的 RAII 封装

**文件**：`socket.h` / `socket.cc`

**职责**：对 Linux socket API 的面向对象封装，管理 fd 生命周期。

```cpp
class Socket {
    explicit Socket(int sockfd);   // 禁止隐式转换
    ~Socket();                      // RAII 自动 close(fd)

    void bindAddress(const sockaddr_in& localaddr);
    void listen();
    int accept(sockaddr_in* peeraddr);  // 返回 connfd
    void shutdownWrite();               // 半关闭

    // socket 选项
    void setTcpNoDelay(bool on);   // 禁用 Nagle，降低延迟
    void setReuseAddr(bool on);    // 地址快速重用
    void setReusePort(bool on);    // 多进程绑定同一端口
    void setKeepAlive(bool on);    // TCP 保活探测
};
```

**关键设计**：
- `explicit` 禁止隐式转换，避免 `int` 意外转成 `Socket`
- 禁止拷贝构造和赋值（`fd` 唯一）
- `accept4` 替代 `accept + fcntl`，**减少一次系统调用**，同时设置 `SOCK_NONBLOCK | SOCK_CLOEXEC`
- `sockaddr_in` 前加 `::` 避免命名冲突（已修复编译错误）

**错误分类**：

| 类型 | 错误码 | 处理 |
|------|--------|------|
| 可恢复 | `EAGAIN`, `ECONNABORTED`, `EINTR`, `EPROTO`, `EPERM`, `EMFILE` | 继续 accept |
| 致命 | `EBADF`, `EFAULT`, `EINVAL`, `ENFILE`, `ENOBUFS`, `ENOMEM` | 记录日志，程序异常 |

---

### 4.2 Buffer —— 应用层缓冲区

**文件**：`buffer.h` / `buffer.cc`

**职责**：管理应用层读写缓冲区，支持 prepend 预留空间。

```cpp
class Buffer {
    static const size_t kCheapPrepend = 8;   // 头部预留 8 字节
    static const size_t kInitialSize = 1024; // 初始 1KB

    size_t readableBytes() const;   // 可读字节数
    size_t writableBytes() const;   // 可写字节数
    size_t prependableBytes() const; // 可回收空间

    void append(const char* data, size_t len);  // 追加数据
    void retrieve(size_t len);                   // 移动读指针
    std::string retrieveAllAsString();           // 取出全部可读数据

    ssize_t readFd(int fd, int* savedErrno);   // 从 fd 读取（readv 优化）
    ssize_t writeFd(int fd, int* savedErrno);  // 写入 fd
};
```

**内存布局**：

```
┌─────────────────┬──────────────────┬─────────────────┐
│  prependable    │    readable      │    writable     │
│  (已读空间)      │   (未读数据)      │   (空闲空间)     │
│                 │                  │                 │
│  0 ──readerIndex_── writerIndex_ ─── buffer_.size() │
│                 │                  │                 │
│  可 prepend     │  peek()/retrieve │  append()       │
│  长度字段        │                  │                 │
└─────────────────┴──────────────────┴─────────────────┘
```

**readv 优化**：
- 使用 `iovec[2]`，主缓冲区 + 栈上临时缓冲区（64KB）
- 避免数据不足时反复 `read` 系统调用
- 大数据直接追加到 `vector`，减少一次拷贝

**makeSpace 策略**：
1. 优先回收 `prependable` 区域（`readerIndex_` 前移）
2. 不够则 `resize` 扩容

---

### 4.3 Channel —— fd 事件分发器

**文件**：`channel.h` / `channel.cc`

**职责**：绑定一个 fd，注册感兴趣的事件，事件发生时调用回调。

```cpp
class Channel {
    void handleEvent();           // 事件分发入口
    void enableReading();         // 注册 EPOLLIN
    void disableReading();        // 注销 EPOLLIN
    void enableWriting();         // 注册 EPOLLOUT
    void disableWriting();        // 注销 EPOLLOUT
    void disableAll();            // 注销所有事件
    void remove();                // 从 epoll 移除

    // 回调绑定
    void setReadCallback(EventCallback cb);
    void setWriteCallback(EventCallback cb);
    void setErrorCallback(EventCallback cb);
    void setCloseCallback(EventCallback cb);
};
```

**事件处理顺序**（关键！）：

```cpp
void Channel::handleEvent() {
    // 1. 先处理 HUP（但如果有数据可读，优先读）
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN)) {
        closeCallback_();   // 对端关闭，且无数据 → 直接关闭
    }

    // 2. 错误事件
    if (revents_ & EPOLLERR) {
        errorCallback_();
    }

    // 3. 可读事件（含 EPOLLRDHUP）
    // EPOLLRDHUP：对端 shutdown 读端，但可能还有数据要读
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
        readCallback_();   // 优先读完数据，再处理关闭
    }

    // 4. 可写事件
    if (revents_ & EPOLLOUT) {
        writeCallback_();
    }
}
```

**设计要点**：
- `EPOLLHUP` 与 `EPOLLIN` 同时触发时，**优先读数据**，避免丢数据
- `EPOLLRDHUP` 归入读回调，确保半关闭场景下数据不丢失
- `index_` 状态：`-1` 未注册 / `1` 已注册 / `2` 已删除

---

### 4.4 EventLoop —— 事件循环核心

**文件**：`event_loop.h` / `event_loop.cc`

**职责**：事件循环、定时器管理、跨线程任务投递。

```cpp
class EventLoop {
    void loop();                    // 事件循环主函数
    void quit();                    // 退出循环
    void updateChannel(Channel*);   // epoll_ctl ADD/MOD
    void removeChannel(Channel*);   // epoll_ctl DEL

    // 跨线程任务投递
    void runInLoop(std::function<void()> cb);   // Loop 线程直接执行，其他线程投递
    void queueInLoop(std::function<void()> cb);   // 加入队列，唤醒执行

    // 定时器
    void runEvery(double intervalSeconds, std::function<void()> cb);  // 周期任务
    void runAfter(double delaySeconds, std::function<void()> cb);     // 一次性任务
};
```

**loop() 主循环**：

```
while (!quit_) {
    1. epoll_wait(timeout=10s)      → 获取就绪事件
    2. 遍历 events_，调用 channel->handleEvent()  → 处理 IO
    3. processTimers()              → 执行到期定时器
    4. doPendingFunctors()          → 执行跨线程投递的回调
}
```

**跨线程唤醒机制**（`eventfd`）：

```
其他线程调用 queueInLoop(cb)
    │
    ▼
┌─────────────────┐
│ 加锁 → push cb  │
│ 到 pending队列   │
└────────┬────────┘
         │
         ▼
    wakeup() ──▶ write(eventfd, 1)  → 唤醒 epoll_wait
         │
         ▼
    EventLoop 线程被唤醒
         │
    doPendingFunctors()
    ┌─────────────────┐
    │ 加锁 → swap(队列) │  ← O(1)，锁内只交换指针
    │ 锁外执行回调      │  ← 避免回调耗时阻塞其他线程投递
    └─────────────────┘
```

**定时器实现**：
- 使用 `std::priority_queue<TimerTask, ..., std::greater<>>`（最小堆）
- `nextRun`：下次执行时间点
- `interval`：`0` 表示一次性，`>0` 表示周期性
- **修复**：`processTimers` 中加了 `interval.count() > 0` 判断，避免一次性任务无限循环

**线程安全断言**：
- `assertInLoopThread()`：检查当前线程是否是 EventLoop 所属线程
- `__thread EventLoop* t_loopInThisThread`：TLS 记录当前线程的 Loop 指针

---

### 4.5 EventLoopThread —— IO 线程封装

**文件**：`event_loop_thread.h` / `event_loop_thread.cc`

**职责**：封装一个线程，线程内创建并运行 EventLoop。

```cpp
class EventLoopThread {
    EventLoop* startLoop();   // 启动线程，返回 Loop 指针（阻塞等待线程就绪）
    void stop();              // 退出线程
};
```

**启动流程**：

```
主线程调用 startLoop()
    │
    ▼
创建子线程 ──▶ threadFunc()
                  │
                  ▼
            创建 EventLoop loop
            执行 callback_(&loop)   ← 线程初始化回调
                  │
                  ▼
            加锁 → loop_ = &loop
            cond_.notify_one()       ← 通知主线程
                  │
                  ▼
            loop.loop()             ← 阻塞事件循环
```

**析构安全**：
- `~EventLoopThread()` 调用 `loop_->quit()` + `thread_.join()`
- 确保子线程先退出，再销毁对象

---

### 4.6 EventLoopThreadPool —— IO 线程池

**文件**：`event_loop_thread_pool.h` / `event_loop_thread_pool.cc`

**职责**：管理多个 EventLoopThread，提供负载均衡分发。

```cpp
class EventLoopThreadPool {
    void setThreadNum(int numThreads);  // 设置 IO 线程数
    void start(const ThreadInitCallback& cb);  // 启动所有线程

    EventLoop* getNextLoop();           // 轮询分发（round-robin）
    EventLoop* getLoopForHash(size_t hashCode);  // 哈希分发（一致性哈希场景）
};
```

**分发策略**：

| 方法 | 适用场景 | 说明 |
|------|---------|------|
| `getNextLoop()` | 普通连接 | 轮询，负载均衡 |
| `getLoopForHash(hashCode)` | 一致性哈希 | 相同 hashCode 总是分到同一个 Loop |

**注意**：`getNextLoop()` 必须在 `baseLoop` 线程调用（有 `assertInLoopThread`），防止跨线程竞争 `next_` 索引。

---

### 4.7 Acceptor —— 监听新连接

**文件**：`acceptor.h` / `acceptor.cc`

**职责**：封装监听 socket，accept 新连接后回调给上层。

```cpp
class Acceptor {
    using NewConnectionCallback = std::function<void(int sockfd, const sockaddr_in&)>;

    void listen();  // 启动监听
    void setNewConnectionCallback(NewConnectionCallback cb);
};
```

**关键设计**：
- 监听 fd 与连接 fd **分离**：监听 fd 在主线程（baseLoop），连接 fd 分发到 IO 线程
- `SOCK_NONBLOCK | SOCK_CLOEXEC`：创建时即非阻塞 + close-on-exec
- `SO_REUSEADDR | SO_REUSEPORT`：地址快速重用，支持多进程

**handleRead 流程**：

```
accept4() 获取 connfd
    │
    ├─ connfd < 0 → 错误分类处理（可恢复/致命）
    │
    └─ connfd >= 0
         │
         ▼
    调用 newConnectionCallback_(connfd, peerAddr)
         │
         ▼
    上层（TcpServer）接管 connfd，创建 TcpConnection
```

---

### 4.8 TcpConnection —— 连接生命周期管理

**文件**：`tcp_connection.h` / `tcp_connection.cc`

**职责**：管理单个 TCP 连接的完整生命周期，包括读写、关闭、超时检测。

```cpp
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
    // 状态机
    enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };

    void connectEstablished();   // 连接建立：注册 Channel、回调
    void connectDestroyed();     // 连接销毁：注销 Channel、清理

    void send(const std::string& message);   // 跨线程安全发送
    void shutdown();                          // 优雅关闭（半关闭）
    void forceClose();                        // 强制关闭

    // idle 超时检测
    void setIdleTimeout(int seconds);
    bool checkIdleTimeout();
};
```

**状态机**：

```
        ┌─────────────┐
        │ kConnecting │  ← 构造后初始状态
        └──────┬──────┘
               │ connectEstablished()
               ▼
        ┌─────────────┐
        │  kConnected  │  ← 正常读写状态
        └──────┬──────┘
               │ shutdown() / handleClose()
               ▼
        ┌─────────────┐
        │kDisconnecting│ ← 正在断开（outputBuffer_ 可能还有数据）
        └──────┬──────┘
               │ outputBuffer_ 写完 / forceClose()
               ▼
        ┌─────────────┐
        │kDisconnected │  ← 已断开，等待析构
        └─────────────┘
```

**发送流程（跨线程安全）**：

```
业务线程调用 send(msg)
    │
    ├─ 在 Loop 线程 → 直接 sendInLoop(msg)
    │
    └─ 不在 Loop 线程 → runInLoop(bind(&sendInLoop, msg))
                              │
                              ▼
                        sendInLoop(msg)
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
            outputBuffer_ 为空      outputBuffer_ 有数据
                    │                   │
                    ▼                   ▼
            直接 write(fd)           追加到 outputBuffer_
            未写完部分入 buffer      注册 EPOLLOUT
                    │                   │
                    ▼                   ▼
            注册 EPOLLOUT（如有剩余）   等 epoll 触发 handleWrite
```

**handleWrite 流程**：

```
epoll 触发 EPOLLOUT
    │
    ▼
write(fd, outputBuffer_.peek(), readableBytes)
    │
    ├─ n > 0: retrieve(n) 移动读指针
    │   ├─ readableBytes == 0: disableWriting()（避免 busy loop）
    │   │   ├─ state == kDisconnecting: shutdownInLoop()（继续关闭）
    │   │   └─ 触发 writeCompleteCallback_
    │   └─ readableBytes > 0: 继续等下次 EPOLLOUT
    │
    └─ n < 0: 错误处理
```

**idle 超时检测**：
- `setIdleTimeout(seconds)`：设置超时时间
- `updateActiveTime()`：每次读写更新活跃时间
- `checkIdleTimeout()`：定时检查，超时调用 `forceClose()`
- **关键修复**：lambda 捕获 `shared_from_this()` 延长生命周期，避免 `Segmentation Fault`

**生命周期管理（核心！）**：

```cpp
// handleClose 中延长生命周期
guardThis = shared_from_this();  // 引用计数 +1
connectionCallback_(guardThis);  // 回调可能引用
closeCallback_(guardThis);       // TcpServer::removeConnection 可能异步
// 函数结束，guardThis 销毁，引用计数 -1
// 如果此时无其他引用，才真正析构
```

---

### 4.9 TcpServer —— 主从 Reactor 整合

**文件**：`tcp_server.h` / `tcp_server.cc`

**职责**：整合 Acceptor、ThreadPool、Connection 管理，提供完整服务端。

```cpp
class TcpServer {
    void setThreadNum(int numThreads);  // 设置 IO 线程数
    void start();                        // 启动监听 + 线程池
    void setIdleTimeout(int seconds);    // 全局 idle 超时

    // 回调设置
    void setConnectionCallback(ConnectionCallback cb);
    void setMessageCallback(MessageCallback cb);
    void setWriteCompleteCallback(WriteCompleteCallback cb);
};
```

**启动流程**：

```
start()
    │
    ├─ started_ = true
    ├─ threadPool_->start()           → 启动所有 IO 线程
    ├─ idleTimeout > 0:
    │   └─ runEvery(5s, checkIdle)    → 每 5 秒检查所有连接超时
    └─ acceptor_->listen()            → 开始监听端口
```

**新连接处理（newConnection）**：

```
Acceptor::handleRead 触发 newConnection(sockfd, peerAddr)
    │
    ▼
1. 生成唯一连接名："IP#id"（如 "127.0.0.1#1"）
2. 从 threadPool_->getNextLoop() 获取 ioLoop
3. getsockname() 获取本地地址
4. 创建 TcpConnection(ioLoop, name, sockfd, localAddr, peerAddr)
5. 设置 idle 超时（如启用）
6. connections_[name] = conn       → 加入连接表
7. 设置回调（connection/message/writeComplete/close）
8. ioLoop->runInLoop(bind(&connectEstablished, conn))
         │
         ▼
    在 ioLoop 线程执行 connectEstablished
         │
         ▼
    注册 Channel 到 epoll，开始监听读写事件
```

**连接移除（跨线程安全）**：

```
任何线程调用 removeConnection(conn)
    │
    ▼
loop_->runInLoop(bind(&removeConnectionInLoop, conn))
    │
    ▼
removeConnectionInLoop(conn)        ← 在 baseLoop 线程执行
    │
    ├─ connections_.erase(conn->name())   → 从 map 移除
    │
    └─ ioLoop->queueInLoop([conn]() {     → 投递到 ioLoop
           conn->connectDestroyed();       → 注销 Channel
       })
```

**析构安全**：

```cpp
~TcpServer() {
    for (auto& item : connections_) {
        TcpConnectionPtr conn = item.second;   // 拷贝 shared_ptr，引用计数 +1
        item.second.reset();                    // map 里置空
        // conn 还持有，不会析构
        conn->getLoop()->runInLoop(bind(&connectDestroyed, conn));
        // 投递到 ioLoop，等其执行完才真正释放
    }
}
```

---

### 4.10 TcpClient —— 客户端底座（待实现）

**文件**：`tcp_client.h`（只有头文件，无实现）

**设计目标**：
- 基于 `Connector` 实现非阻塞 connect + 自动重连
- 为 `RpcAsyncClient` 提供底层网络支撑
- 支持连接池（Week 2）

```cpp
class TcpClient {
    void connect();      // 发起连接
    void disconnect();   // 断开连接
    void stop();         // 停止（取消重连）

    std::shared_ptr<TcpConnection> connection() const;

    // 回调
    void setConnectionCallback(ConnectionCallback cb);
    void setMessageCallback(MessageCallback cb);
    void setWriteCompleteCallback(WriteCompleteCallback cb);
};
```

**类关系（Week 2 实现）**：

```
RpcAsyncClient
    │
    ▼
TcpClient ──▶ Connector ──▶ Socket/Channel
    │
    ▼
ConnectionPool（Week 2 后期接入）
```

---

## 5. 线程模型

### 5.1 One Loop Per Thread

```
主线程（baseLoop/accept loop）
    ├── 运行 Acceptor
    ├── accept 新连接
    ├── 轮询分发到 subLoop
    ├── 管理 connections_ map
    └── 执行 idle 超时检测定时器

IO 线程 0（subLoop-0）
    ├── epoll_wait 等待事件
    ├── 处理 conn-0 的读写
    ├── 处理 conn-2 的读写
    └── 执行该 Loop 的 pending functors

IO 线程 1（subLoop-1）
    ├── epoll_wait 等待事件
    ├── 处理 conn-1 的读写
    ├── 处理 conn-3 的读写
    └── 执行该 Loop 的 pending functors
```

**优势**：
- 每个连接绑定到一个线程，**无锁处理读写**
- 避免多线程竞争 fd，减少锁开销
- 利用多核性能

### 5.2 跨线程通信

| 场景 | 调用方 | 接收方 | 机制 |
|------|--------|--------|------|
| 业务线程 send | 业务线程 | TcpConnection 的 Loop | `runInLoop` → `eventfd` 唤醒 |
| 移除连接 | ioLoop / 任意线程 | baseLoop | `runInLoop` → `eventfd` 唤醒 |
| 销毁连接 | baseLoop | ioLoop | `queueInLoop` → `eventfd` 唤醒 |
| 定时任务 | 任意线程 | 同线程 Loop | `runEvery` / `runAfter` |

---

## 6. 踩坑记录

### 6.1 socket.cc 编译错误

**问题**：`sockaddr_in` 未定义，编译报错。

**原因**：Windows 下 `sockaddr_in` 可能在全局命名空间，Linux 下需要 `::sockaddr_in` 或包含 `<netinet/in.h>`。

**修复**：
```cpp
// socket.cc
#include <netinet/in.h>  // ← 已添加

void Socket::bindAddress(const ::sockaddr_in& localaddr) {  // ← 加 :: 前缀
```

### 6.2 EventLoop 回调队列多线程竞争

**问题**：`queueInLoop` 和 `doPendingFunctors` 跨线程竞争 `pendingFunctors_`。

**修复**：
```cpp
void EventLoop::doPendingFunctors() {
    std::vector<std::function<void()>> functors;
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        functors.swap(pendingFunctors_);  // O(1)，锁内只交换指针
    }
    // 锁外执行回调（可能耗时）
    for (const auto& f : functors) { f(); }
}
```

### 6.3 ConsistentHash 多线程读写崩溃

**问题**：多线程同时读写哈希环，导致崩溃。

**修复**：`std::mutex` 保护读写操作。

### 6.4 idle 超时断开 Segmentation Fault

**问题**：定时器回调中 `TcpConnection` 已被销毁，访问悬空指针。

**修复**：lambda 捕获 `shared_ptr` 延长生命周期。
```cpp
// 错误：this 可能已销毁
loop_->runInLoop([this]() { handleClose(); });

// 正确：shared_from_this() 延长生命周期
loop_->queueInLoop(std::bind(&TcpConnection::handleClose, shared_from_this()));
```

### 6.5 连接关闭日志打印 "new connection"

**问题**：连接关闭时错误打印 "new connection"。

**修复**：增加 `connected()` 方法判断状态。
```cpp
bool connected() const { return state_ == kConnected; }
```

### 6.6 定时器 processTimers 无限循环

**问题**：一次性任务（interval=0）被重复入队，导致无限循环。

**修复**：
```cpp
if (task.interval.count() > 0) {  // ← 加判断
    task.nextRun = task.nextRun + task.interval;
    timers_.push(task);
}
```

---

## 7. 待完善（Week 2）

### 7.1 Connector（7.14）

- 非阻塞 connect + epoll 监听可写
- `getsockopt(SO_ERROR)` 检查连接结果
- 指数退避重连（1s, 2s, 4s, 8s... 上限 30s）
- 停止时 cancel 定时器

### 7.2 TcpClient.cc（7.14）

- 基于 Connector 的客户端实现
- 连接建立/断开/重连管理
- 为 RpcAsyncClient 提供底座

### 7.3 ConnectionPool（7.17）

- 基于 TcpClient 的连接池
- 连接复用、心跳保活、异常恢复
- 客户端维护到每个服务节点的连接池

### 7.4 其他分布式特性

| 特性 | 日期 | 文件 |
|------|------|------|
| 内存版注册中心 | 7.15 | `discovery/memory_registry.h/cc` |
| etcd 客户端 | 7.16 | `discovery/etcd_client.h/cc` |
| 轮询负载均衡 | 7.18 | `loadbalance/round_robin.h/cc` |
| 熔断器 | 7.19 | `server/circuit_breaker.h/cc` |
| 令牌桶限流 | 7.20 | `server/token_bucket.h/cc` |
| 分布式限流 | 7.21 | `server/distributed_limiter.h/cc` |

---

## 附录：关键代码片段索引

| 功能 | 文件 | 行号范围 | 说明 |
|------|------|---------|------|
| accept4 优化 | socket.cc | ~45 | 减少一次系统调用 |
| readv 优化 | buffer.cc | ~75 | 双缓冲区读取 |
| 事件分发顺序 | channel.cc | ~30 | HUP/ERR/IN/OUT |
| 跨线程唤醒 | event_loop.cc | ~130 | eventfd 机制 |
| swap 减少锁竞争 | event_loop.cc | ~160 | pending functors |
| 定时器修复 | event_loop.cc | ~85 | interval > 0 判断 |
| 生命周期延长 | tcp_connection.cc | ~180 | shared_ptr 捕获 |
| 连接移除安全 | tcp_server.cc | ~95 | 跨线程 + 引用计数 |
| idle 超时检测 | tcp_server.cc | ~40 | runEvery 定时检查 |

---

> **面试要点**：
> 1. **Reactor 模式**：事件驱动，非阻塞 IO + epoll，回调处理
> 2. **One Loop Per Thread**：每个连接一个线程，无锁，利用多核
> 3. **生命周期管理**：shared_ptr + enable_shared_from_this，lambda 捕获延长生命周期
> 4. **跨线程安全**：runInLoop/queueInLoop + eventfd 唤醒，swap 减少锁竞争
> 5. **Buffer 设计**：prepend 预留空间，readv 优化，makeSpace 策略
> 6. **状态机**：TcpConnection 四状态，优雅关闭 vs 强制关闭
