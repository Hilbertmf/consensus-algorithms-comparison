# network/CMakeLists.txt
add_library(node_network STATIC
    grpc_messaging.cpp
)

target_inclule_directories(node_network PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/proto
)

target_link_libraries(node_network PUBLIC
    node_proto
    gRPC::grpc++
    protobuf::libprotobuf
)