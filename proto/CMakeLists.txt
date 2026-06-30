# proto/CMakeLists.txt
protobuf_generate(TARGET node_proto LANGUAGE cpp)
grpc_generate(TARGET node_proto LANGUAGE cpp)

add_library(node_proto STATIC ${node_proto_SRCS} ${node_proto_GRPCS_SRCS})
target_include_directories(node_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(node_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)