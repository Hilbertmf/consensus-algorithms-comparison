#pragma once
#include "messaging_interface.hpp"
#include "drone.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <mutex>

namespace node_consensus {

class GrpcMessaging : public MessagingLayer {
public:

    GrpcMessaging(const std::string& own_id, int port);
    ~GrpcMessaging() override;

    // interface
    bool sendTo(const std::string& target_id, const Message& msg) override;
    bool boardcast(const Message& msg) override;
    bool startReceiving(std::function<void(const Message&)> onReceive) override;
    std::string getOwnId() const override;
    void registerPeer(const std::string& id, const std::string& address) override;

private:

    // grpc service implementation
    class NodeServiceImpl final : public node::NodeService::Service {
    public:
        explicit NodeServiceImpl(std::function<void(const Message&)> callback);
        grpc::Status DeliverMessage(grpc::ServerContext* context,
        const node::NodeMessage* request,
        node::Ack* response) override;

    private:
        std::function<void(const Message&)> on_receive_callback;
    };

    std::string own_id_;
    int port_;
    std::unique_ptr<gprc::Server> server_;
    std::unique_ptr<NodeServiceImpl> service_impl_;
    std::unique_ptr<std::thread> server_thread_;
    std::atomic<bool> running_(false);

    // Peer mapping: id -> grpc::Channel (or stub)
    std::unordered_map<std::string, std::unique_ptr<drone::DroneService::Stub>> peer_stubs_;
    std::unordered_map<std::string, std::string> peer_addresses_;
    mutable std::mutex peer_mutex_;
};
    

}

}