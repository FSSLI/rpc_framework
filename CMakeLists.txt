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
    src/network/event_loop_thread.cc      # ← 新增
    src/network/event_loop_thread_pool.cc # ← 新增
)

set(CODEC_SRCS
    src/codec/rpc_codec.cc
)

set(PROTO_SRCS
    src/protocol/rpc_service.pb.cc
)

set(SERVER_SRCS
    src/server/rpc_service.cc
    src/server/rpc_server.cc
)

set(CLIENT_SRCS
    src/client/rpc_sync_client.cc
    src/client/rpc_async_client.cc  # 新增
)

set(LOADBALANCE_SRCS
    src/loadbalance/consistent_hash.cc
)

add_library(network ${NETWORK_SRCS} ${CODEC_SRCS} ${PROTO_SRCS} ${CLIENT_SRCS} ${SERVER_SRCS} ${LOADBALANCE_SRCS})
target_link_libraries(network protobuf pthread)

# add_executable(echo_server examples/echo/echo_server.cc)
# target_link_libraries(echo_server network)

# add_executable(echo_client examples/echo/echo_client.cc)
# target_link_libraries(echo_client network)

add_executable(rpc_sync_test examples/rpc/rpc_sync_test.cc)
target_link_libraries(rpc_sync_test network)

# 新的 rpc_server 示例
add_executable(rpc_server examples/rpc/rpc_server_main.cc examples/rpc/echo_service.cc)
target_link_libraries(rpc_server network)

add_executable(rpc_async_test examples/rpc/rpc_async_test.cc)
target_link_libraries(rpc_async_test network)

add_executable(test_consistent_hash tests/test_consistent_hash.cc)
target_link_libraries(test_consistent_hash network)