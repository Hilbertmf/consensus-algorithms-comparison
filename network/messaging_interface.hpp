#pragma once
#include <string>
#include <functional>
#include <vector>

namespace node_consensus {
    struct Message {
        std::string sender_id;
        std::string receiver_id;
        std::string message_type;
        std::string payload; // serialized JSON or bin
        uint64_t timestamp;

    };
}

class MessagingLayer {
public:
    virtual ~MessagingLayer() = default;

    // point-to-point send
    virtual bool sendTo(const std::string& target_id, const Message& msg) = 0;

    virtual bool broadcast(const Message& msg) = 0;

    virtual void startReceiving(std::function<void(const Message&)> onReceive) = 0;

    virtual void stop() = 0;

    virtual std::string getOwnId() const = 0;

    // called during topology setup
    virtual void registerPeer(const std::string& id, const std::string& address) = 0;

    virtual std::vector<std::string> getPeerIds() const = 0;
}