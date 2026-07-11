// src/network/buffer.h
#ifndef BUFFER_H
#define BUFFER_H

#include <vector>
#include <string>
#include <algorithm>

namespace rpc {

class Buffer {
public:
    static const size_t kCheapPrepend = 8;  // 头部预留空间，用于 prepend 长度字段（如 4 字节 Length）
    static const size_t kInitialSize = 1024;  //初始化buffer大小

    Buffer();
    
    size_t readableBytes() const;  // 当前已写入但尚未读取的字节数 = writerIndex_ - readerIndex_
    size_t writableBytes() const;  //可写字节数
    size_t prependableBytes() const;  // 已被读取的空间大小 = readerIndex_，可被回收或 prepend 使用

    const char* peek() const;  //当前可读位置
    void retrieve(size_t len);  // 移动读指针，标记 len 字节为已读（数据不删除，可被覆盖）
    void retrieveAll();  // 重置读写指针到初始位置（kCheapPrepend），回收全部空间
    std::string retrieveAllAsString();  // 取出全部可读数据，返回 string，同时移动读指针
    std::string retrieveAsString(size_t len); // 取出 len 字节数据，返回 string，同时移动读指针

    void append(const char* data, size_t len);  // 追加数据到 buffer 尾部
    void append(const std::string& str);   // 追加 string 到 buffer 尾部

    // 确保可写空间
    void ensureWritableBytes(size_t len);  

    // 从 fd 读取数据
    ssize_t readFd(int fd, int* savedErrno); // 从 fd 读取数据到 buffer，使用 readv 优化（减少一次拷贝）

    // 写入 fd
    ssize_t writeFd(int fd, int* savedErrno);

    char* beginWrite();  //可写位置

private:
    char* begin();  //buffer起始位置
    const char* begin() const;  //buffer起始位置
    void makeSpace(size_t len);  // 确保 len 字节可写空间：优先回收 prependable 区域，不够则 resize

    std::vector<char> buffer_;  //缓冲区实现
    size_t readerIndex_;  //读指针
    size_t writerIndex_;  //写指针
};

} // namespace rpc

#endif