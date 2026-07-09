cmake_minimum_required(VERSION 3.10)
project(rpc_framework)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_SOURCE_DIR}/src)

set(NETWORK_SRCS
    src/network/event_loop.cc
    src/network/channel.cc
    src/network/buffer.cc
    src/network/socket.cc
    src/network/acceptor.cc
    src/network/tcp_connection.cc
    src/network/tcp_server.cc
)

set(CODEC_SRCS
    src/codec/rpc_codec.cc
)

set(PROTO_SRCS
    src/protocol/rpc_service.pb.cc
)

add_library(network ${NETWORK_SRCS} ${CODEC_SRCS} ${PROTO_SRCS})
target_link_libraries(network protobuf)

add_executable(echo_server examples/echo/echo_server.cc)
target_link_libraries(echo_server network)

add_executable(echo_client examples/echo/echo_client.cc)
target_link_libraries(echo_client network)