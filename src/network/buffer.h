// src/network/buffer.h
#ifndef BUFFER_H
#define BUFFER_H

#include <vector>
#include <string>
#include <algorithm>

namespace rpc {

class Buffer {
public:
    static const size_t kCheapPrepend = 8;
    static const size_t kInitialSize = 1024;

    Buffer();
    
    size_t readableBytes() const;
    size_t writableBytes() const;
    size_t prependableBytes() const;

    const char* peek() const;
    void retrieve(size_t len);
    void retrieveAll();
    std::string retrieveAllAsString();
    std::string retrieveAsString(size_t len);

    void append(const char* data, size_t len);
    void append(const std::string& str);

    // 确保可写空间
    void ensureWritableBytes(size_t len);

    // 从 fd 读取数据
    ssize_t readFd(int fd, int* savedErrno);

    // 写入 fd
    ssize_t writeFd(int fd, int* savedErrno);

    char* beginWrite();

private:
    char* begin();
    const char* begin() const;
    void makeSpace(size_t len);

    std::vector<char> buffer_;
    size_t readerIndex_;
    size_t writerIndex_;
};

} // namespace rpc

#endif