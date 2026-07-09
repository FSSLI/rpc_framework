// src/network/buffer.cc
#include "buffer.h"
#include <sys/uio.h>
#include <errno.h>
#include <unistd.h>   // ← 加这行
#include <iostream>

namespace rpc {

Buffer::Buffer()
    : buffer_(kCheapPrepend + kInitialSize),
      readerIndex_(kCheapPrepend),
      writerIndex_(kCheapPrepend) {
}

size_t Buffer::readableBytes() const {
    return writerIndex_ - readerIndex_;
}

size_t Buffer::writableBytes() const {
    return buffer_.size() - writerIndex_;
}

size_t Buffer::prependableBytes() const {
    return readerIndex_;
}

const char* Buffer::peek() const {
    return begin() + readerIndex_;
}

void Buffer::retrieve(size_t len) {
    if (len < readableBytes()) {
        readerIndex_ += len;
    } else {
        retrieveAll();
    }
}

void Buffer::retrieveAll() {
    readerIndex_ = kCheapPrepend;
    writerIndex_ = kCheapPrepend;
}

std::string Buffer::retrieveAllAsString() {
    std::string result(peek(), readableBytes());
    retrieveAll();
    return result;
}

// network/buffer.cc 里加：
std::string Buffer::retrieveAsString(size_t len) {
    std::string result(peek(), len);
    retrieve(len);
    return result;
}

void Buffer::append(const char* data, size_t len) {
    ensureWritableBytes(len);
    std::copy(data, data + len, beginWrite());
    writerIndex_ += len;
}

void Buffer::append(const std::string& str) {
    append(str.data(), str.size());
}

void Buffer::ensureWritableBytes(size_t len) {
    if (writableBytes() < len) {
        makeSpace(len);
    }
}

ssize_t Buffer::readFd(int fd, int* savedErrno) {
    char extrabuf[65536];
    struct iovec vec[2];
    
    const size_t writable = writableBytes();
    vec[0].iov_base = begin() + writerIndex_;
    vec[0].iov_len = writable;
    vec[1].iov_base = extrabuf;
    vec[1].iov_len = sizeof(extrabuf);
    
    const int iovcnt = (writable < sizeof(extrabuf)) ? 2 : 1;
    const ssize_t n = ::readv(fd, vec, iovcnt);
    
    if (n < 0) {
        *savedErrno = errno;
    } else if (static_cast<size_t>(n) <= writable) {
        writerIndex_ += n;
        std::cout << "writerIndex_=" << writerIndex_ << " readerIndex_=" << readerIndex_ << std::endl;  // ← 加
    } else {
        writerIndex_ = buffer_.size();
        append(extrabuf, n - writable);
    }
    
    return n;
}

ssize_t Buffer::writeFd(int fd, int* savedErrno) {
    ssize_t n = ::write(fd, peek(), readableBytes());
    if (n < 0) {
        *savedErrno = errno;
    }
    return n;
}

char* Buffer::begin() {
    return &*buffer_.begin();
}

const char* Buffer::begin() const {
    return &*buffer_.begin();
}

void Buffer::makeSpace(size_t len) {
    if (writableBytes() + prependableBytes() < len + kCheapPrepend) {
        buffer_.resize(writerIndex_ + len);
    } else {
        size_t readable = readableBytes();
        std::copy(begin() + readerIndex_,
                  begin() + writerIndex_,
                  begin() + kCheapPrepend);
        readerIndex_ = kCheapPrepend;
        writerIndex_ = readerIndex_ + readable;
    }
}

char* Buffer::beginWrite() {
    return begin() + writerIndex_;
}

} // namespace rpc