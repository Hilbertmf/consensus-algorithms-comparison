// network/grpc_messaging.cpp
#include "grpc_messaging.hpp"
#include <grpcpp/create_channel.h>
#include <grpcpp/server_builder.h>
#include <iostream>
#include <chrono>

namespace node_consensus {

GrpcMessaging::NodeServiceImpl::NodeServiceImpl(std::function<void(const Message&)> callback)
    : on_receive_callback(std::move(callback)) {}

grpc::Status GrpcMessaging::NodeServiceImpl::DeliverMessage(grpc::ServerContext* context,
                                                             const ::NodeMessage* request,
                                                             Node::Ack* response) {
    // Convert proto to internal Message
    Message msg;
    msg.sender_id = request->sender_id();
    msg.receiver_id = request->receiver_id();
    msg.message_type = request->message_type();
    msg.payload = request->payload();
    msg.timestamp = request->timestamp();

    // Pass to the upper layer (consensus algorithm)
    if (on_receive_callback) {
        on_receive_callback(msg);
    }

    response->set_success(true);
    return grpc::Status::OK;
}

GrpcMessaging::GrpcMessaging(const std::string& own_id, int port)
    : own_id_(own_id), port_(port) {}

GrpcMessaging::~GrpcMessaging() {
    stop();
}

void GrpcMessaging::registerPeer(const std::string& id, const std::string& address) {
    std::lock_guard<std::mutex> lock(peer_mutex_);
    peer_addresses_[id] = address;
    // Create a channel to this peer (insecure for emulation)
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    peer_stubs_[id] = node::NodeService::NewStub(channel);
}

bool GrpcMessaging::sendTo(const std::string& target_id, const Message& msg) {
    std::lock_guard<std::mutex> lock(peer_mutex_);
    auto it = peer_stubs_.find(target_id);
    if (it == peer_stubs_.end()) {
        std::cerr << "[ERROR] Unknown peer: " << target_id << std::endl;
        return false;
    }

    // Build the proto message
    node::NodeMessage proto_msg;
    proto_msg.set_sender_id(msg.sender_id.empty() ? own_id_ : msg.sender_id);
    proto_msg.set_receiver_id(target_id);
    proto_msg.set_message_type(msg.message_type);
    proto_msg.set_payload(msg.payload);
    proto_msg.set_timestamp(msg.timestamp);

    // Call the RPC
    grpc::ClientContext context;
    node::Ack ack;
    grpc::Status status = it->second->DeliverMessage(&context, proto_msg, &ack);

    if (!status.ok()) {
        std::cerr << "[ERROR] sendTo failed for " << target_id
                  << ": " << status.error_message() << std::endl;
        return false;
    }
    return ack.success();
}

bool GrpcMessaging::broadcast(const Message& msg) {
    std::lock_guard<std::mutex> lock(peer_mutex_);
    bool all_success = true;
    for (const auto& [id, stub] : peer_stubs_) {
        // Skip self (we don't send to ourselves)
        if (id == own_id_) continue;

        node::NodeMessage proto_msg;
        proto_msg.set_sender_id(own_id_);
        proto_msg.set_receiver_id(id);
        proto_msg.set_message_type(msg.message_type);
        proto_msg.set_payload(msg.payload);
        proto_msg.set_timestamp(msg.timestamp);

        grpc::ClientContext context;
        node::Ack ack;
        grpc::Status status = stub->DeliverMessage(&context, proto_msg, &ack);
        if (!status.ok() || !ack.success()) {
            all_success = false;
            std::cerr << "[WARN] Broadcast to " << id << " failed" << std::endl;
        }
    }
    return all_success;
}

void GrpcMessaging::startReceiving(std::function<void(const Message&)> onReceive) {
    if (running_) return;

    running_ = true;
    service_impl_ = std::make_unique<NodeServiceImpl>(std::move(onReceive));

    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + std::to_string(port_), grpc::InsecureServerCredentials());
    builder.RegisterService(service_impl_.get());

    server_ = builder.BuildAndStart();
    if (!server_) {
        std::cerr << "[ERROR] Failed to start gRPC server on port " << port_ << std::endl;
        running_ = false;
        return;
    }

    std::cout << "[INFO] gRPC server listening on port " << port_ << std::endl;

    // Run server in a background thread (blocking)
    server_thread_ = std::make_unique<std::thread>([this]() {
        server_->Wait();
    });
}

void GrpcMessaging::stop() {
    if (!running_) return;
    running_ = false;

    if (server_) {
        server_->Shutdown();
        if (server_thread_ && server_thread_->joinable()) {
            server_thread_->join();
        }
        server_.reset();
    }
    server_thread_.reset();
    service_impl_.reset();
}

std::string GrpcMessaging::getOwnId() const {
    return own_id_;
}

std::vector<std::string> GrpcMessaging::getPeerIds() const {
    std::lock_guard<std::mutex> lock(peer_mutex_);
    std::vector<std::string> ids;
    for (const auto& [id, _] : peer_addresses_) {
        if (id != own_id_) ids.push_back(id);
    }
    return ids;
}

}