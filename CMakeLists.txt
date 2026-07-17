cmake_minimum_required(VERSION 3.10)
project(rpc_framework)

# === 日志级别控制 ===

option(RPC_SILENT "Disable all RPC log output (for benchmarks)" OFF)
option(RPC_DEBUG  "Enable verbose debug log"                      OFF)

if(RPC_SILENT)
    add_definitions(-DRPC_SILENT)
elseif(RPC_DEBUG)
    add_definitions(-DRPC_DEBUG)
endif()

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

# etcd 服务发现（可选，需要 etcd-cpp-apiv3 + nlohmann-json）
option(WITH_ETCD "Build with etcd service discovery" OFF)
if(WITH_ETCD)
    add_definitions(-DHAS_ETCD)
    list(APPEND DISCOVERY_SRCS src/discovery/etcd_registry.cc)

    # 加上这一行，让 CMake 能找到 etcd-cpp-apiv3 的 cmake config
    find_package(etcd-cpp-api REQUIRED)
endif()

set(POOL_SRCS
    src/pool/connection_pool.cc
)

set(CIRCUIT_BREAKER_SRCS
    src/circuit_breaker/circuit_breaker.cc
)

set(RATE_LIMITER_SRCS
    src/rate_limiter/token_bucket.cc
)

set(INTERCEPTOR_SRCS
    src/interceptor/interceptor.cc
    src/interceptor/builtin_interceptors.cc
)

# Redis 令牌桶（可选，需要 hiredis）
option(WITH_REDIS "Build with Redis token bucket" OFF)
if(WITH_REDIS)
    add_definitions(-DHAS_REDIS)
    list(APPEND RATE_LIMITER_SRCS src/rate_limiter/redis_token_bucket.cc)
endif()

add_library(network ${NETWORK_SRCS} ${CODEC_SRCS} ${PROTO_SRCS} ${CLIENT_SRCS} ${SERVER_SRCS} ${LOADBALANCE_SRCS} ${DISCOVERY_SRCS} ${POOL_SRCS} ${CIRCUIT_BREAKER_SRCS} ${RATE_LIMITER_SRCS} ${INTERCEPTOR_SRCS})
target_link_libraries(network protobuf pthread)

if(WITH_ETCD)
    target_link_libraries(network etcd-cpp-api)
endif()

if(WITH_REDIS)
    target_link_libraries(network hiredis)
endif()

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

add_executable(test_load_balance tests/test_load_balance.cc)
target_link_libraries(test_load_balance network)

add_executable(test_circuit_breaker tests/test_circuit_breaker.cc)
target_link_libraries(test_circuit_breaker network)

add_executable(test_token_bucket tests/test_token_bucket.cc)
target_link_libraries(test_token_bucket network)

add_executable(test_integration tests/test_integration.cc)
target_link_libraries(test_integration network)

add_executable(test_consistent_hash_lb tests/test_consistent_hash_lb.cc)
target_link_libraries(test_consistent_hash_lb network)

add_executable(test_trace_id tests/test_trace_id.cc)
target_link_libraries(test_trace_id network)

add_executable(test_interceptor tests/test_interceptor.cc)
target_link_libraries(test_interceptor network)

add_executable(test_benchmark tests/test_benchmark.cc)
target_link_libraries(test_benchmark network)

# etcd 测试（需 WITH_ETCD=ON + Docker 运行 etcd）
if(WITH_ETCD)
    add_executable(test_etcd_registry tests/test_etcd_registry.cc)
    target_link_libraries(test_etcd_registry network)
endif()

# Redis 测试（需 WITH_REDIS=ON + Docker 运行 Redis）
if(WITH_REDIS)
    add_executable(test_redis_token_bucket tests/test_redis_token_bucket.cc)
    target_link_libraries(test_redis_token_bucket network)
endif()