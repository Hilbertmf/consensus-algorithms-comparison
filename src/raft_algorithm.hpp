// src/raft_algorithm.hpp
#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <atomic>
#include <thread>
#include <mutex>
#include <chrono>
#include <functional>
#include <random>
#include <nlohmann/json.hpp>

#include "../network/messaging_interface.hpp"

using json = nlohmann::json;

namespace node_consensus {

struct LogEntry {
    uint64_t term;
    std::string command;
};

enum class RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

class RaftNode {
public:
    // Constructor: 
    //   - own_id: unique identifier for this node (e.g., "node-1")
    //   - messaging: shared pointer to the messaging layer
    //   - election_timeout_ms: base timeout (actual randomized between 2x and 3x)
    //   - heartbeat_interval_ms: how often leader sends AppendEntries
    RaftNode(const std::string& own_id,
             std::shared_ptr<MessagingLayer> messaging,
             int election_timeout_ms = 1500,
             int heartbeat_interval_ms = 500);

    ~RaftNode();

    void start();

    void stop();

    // Propose a command to the cluster (for clients)
    // Returns true if the command was accepted
    bool propose(const std::string& command);

    std::string getLeaderId() const;

private:
    // Core Raft state
    std::string id_;
    std::shared_ptr<MessagingLayer> messaging_;

    // Persistent state (on all servers)
    uint64_t currentTerm_;
    std::string votedFor_;   // candidateId that received vote in current term
    std::vector<LogEntry> log_;

    // Volatile state (on all servers)
    uint64_t commitIndex_;
    uint64_t lastApplied_;

    // Volatile state (on leaders)
    std::map<std::string, uint64_t> nextIndex_;   // for each peer
    std::map<std::string, uint64_t> matchIndex_;  // for each peer

    RaftState state_;
    std::string leaderId_;
    std::mt19937 rng_; // for random timeouts

    // Configuration
    int electionTimeoutMs_;
    int heartbeatIntervalMs_;

    // Timers and threads
    std::atomic<bool> running_;
    std::thread timerThread_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;

    std::chrono::steady_clock::time_point lastHeartbeatTime_;

    // Message handling
    void onMessage(const Message& msg);
    void handleRequestVote(const json& req);
    void handleRequestVoteResponse(const json& resp);
    void handleAppendEntries(const json& req);
    void handleAppendEntriesResponse(const json& resp);

    // Timer logic
    void timerLoop();
    void resetElectionTimer();
    bool electionTimeoutElapsed() const;

    // Leader-specific functions
    void startLeader();
    void sendHeartbeats();
    void replicateToPeer(const std::string& peer_id);

    // Helper functions
    bool isMoreUpToDate(uint64_t lastLogTerm, uint64_t lastLogIndex) const;
    uint64_t lastLogIndex() const;
    uint64_t lastLogTerm() const;

    // Serialize/deserialize for messages
    json serializeMessage(const std::string& type, const json& payload) const;
    bool deserializeMessage(const Message& msg, std::string& type, json& payload) const;
};

}