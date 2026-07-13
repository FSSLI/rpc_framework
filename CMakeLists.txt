cmake_minimum_required(VERSION 3.10)
project(rpc_framework)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR})  # 添加项目根目录

set(NETWORK_SRCS
    src/network/event_loop.cc
    src/network/channel.cc
    src/network/buffer.cc
    src/network/socket.cc
    src/network/acceptor.cc
    src/network/tcp_connection.cc
    src/network/tcp_server.cc
    src/network/event_loop_thread.cc
    src/network/event_loop_thread_pool.cc
    src/network/connector.cc
    src/network/tcp_client.cc
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
    src/client/rpc_async_client.cc
)

set(LOADBALANCE_SRCS
    src/loadbalance/consistent_hash.cc
)

set(DISCOVERY_SRCS
    src/discovery/memory_registry.cc
)

set(POOL_SRCS
    src/pool/connection_pool.cc
)

add_library(network ${NETWORK_SRCS} ${CODEC_SRCS} ${PROTO_SRCS} ${CLIENT_SRCS} ${SERVER_SRCS} ${LOADBALANCE_SRCS} ${DISCOVERY_SRCS} ${POOL_SRCS})
target_link_libraries(network protobuf pthread)

# 示例程序
add_executable(rpc_sync_test examples/rpc/rpc_sync_test.cc)
target_link_libraries(rpc_sync_test network)

add_executable(rpc_server examples/rpc/rpc_server_main.cc examples/rpc/echo_service.cc)
target_link_libraries(rpc_server network)

add_executable(rpc_async_test examples/rpc/rpc_async_test.cc)
target_link_libraries(rpc_async_test network)

# 测试程序
add_executable(test_consistent_hash tests/test_consistent_hash.cc)
target_link_libraries(test_consistent_hash network)

add_executable(test_connector tests/test_connector.cc)
target_link_libraries(test_connector network)

add_executable(test_memory_registry tests/test_memory_registry.cc)
target_link_libraries(test_memory_registry network)

add_executable(test_service_discovery_e2e tests/test_service_discovery_e2e.cc)
target_link_libraries(test_service_discovery_e2e network)

add_executable(test_connection_pool tests/test_connection_pool.cc)
target_link_libraries(test_connection_pool network)