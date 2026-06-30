cmake_minimum_required(VERSION 3.15)
project(NodeConsensus)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find gRPC and Protobuf
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)

# Generate gRPC code from .proto
add_subdirectory(proto)
add_subdirectory(network)
add_subdirectory(src)

# Link everything into the main executable (or libraries)