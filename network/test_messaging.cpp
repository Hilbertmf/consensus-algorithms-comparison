// network/test_messaging.cpp
#include "grpc_messaging.hpp"
#include <iostream>
#include <thread>
#include <chrono>

using namespace node_consensus;

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <own_id> <port> [peer_id peer_address ...]" << std::endl;
        return 1;
    }

    std::string own_id = argv[1];
    int port = std::stoi(argv[2]);

    auto messaging = std::make_shared<node_consensus::GrpcMessaging>("node-1", 50051);
    // Register peers (you'll do this in Mininet)
    messaging->registerPeer("node-2", "10.0.0.2:50051");
    messaging->registerPeer("node-3", "10.0.0.3:50051");

    GrpcMessaging node(own_id, port);

    // Register peers (passed as pairs: id address)
    for (int i = 3; i + 1 < argc; i += 2) {
        node.registerPeer(argv[i], argv[i+1]);
    }

    // Start receiving
    node.startReceiving([](const Message& msg) {
        std::cout << "[RECEIVED] from " << msg.sender_id
                  << " type: " << msg.message_type
                  << " payload: " << msg.payload << std::endl;
    });

    // If this is node A, send a test message to node B after a short delay
    if (own_id == "node-1") {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        Message test_msg;
        test_msg.sender_id = "node-1";
        test_msg.message_type = "PING";
        test_msg.payload = "Hello from node-1";
        test_msg.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();

        bool ok = node.sendTo("node-2", test_msg);
        std::cout << "[SEND] PING to node-2: " << (ok ? "SUCCESS" : "FAILED") << std::endl;

        // Also test broadcast
        node.broadcast(test_msg);
    }

    node_consensus::RaftNode raft("node-1", messaging);
    raft.start();

    // Propose a command (if leader)
    raft.propose("SET_VALUE 42");

    // Keep running
    std::this_thread::sleep_for(std::chrono::seconds(10));
    node.stop();
    return 0;
}