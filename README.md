# consensus-algorithms-comparison
Comparison of Consensus Algorithms to facilitate coordination among autonomous nodes. It aims to ensure reliable and fault-tolerant decision-making in scenarios with potential communication failures and malicious (Byzantine) behaviors. Includes testing these algorithms within emulated network scenarios to compare their performance and resilience.

# Drone Fleet Commander Selection – Consensus Algorithms Project

## Overview
This project implements and evaluates distributed consensus algorithms for electing a commander in a fleet of autonomous drones operating in a hostile environment. The system must tolerate various failure modes—including crash, omission, timing, and Byzantine faults—and guarantee that all correct drones agree on a single leader before commencing the mission.

## Objectives
- **Implement** three consensus protocols (Raft, PBFT and Lamport‑Shostak‑Pease).
- **Emulate** realistic drone communication using Mininet (network emulator) on a native Linux system.
- **Test** under multiple failure scenarios and node scales to evaluate correctness, performance, and fault tolerance.
- **Analyze** the trade‑offs between algorithms in terms of message complexity, resilience, and implementation complexity.

## System Specifications
- **Host OS**: Kubuntu 22.04 LTS (Kernel 5.15)
- **CPU**: Intel Core i5‑8265U (4 cores / 8 threads)
- **RAM**: 12 GB
- **Emulation**: Mininet (native installation) or Containernet for container‑based nodes
- **Language**: C++ (standard sockets for inter‑node communication)

## Test Scenarios
| Scenario | Nodes | Failure Type | Faulty Nodes |
| :--- | :--- | :--- | :--- |
| A | 5 | Crash / omission / timing | 1 – 3 |
| B | 10 | Crash / omission / timing | 1 – 6 |
| C | 5 | Byzantine | 1 – 2 |
| D | 10 | Byzantine | 1 – 4 |

> **Note**: Due to the theoretical bound `n ≥ 3f + 1` for Byzantine fault tolerance, some combinations (e.g., 5 nodes with 2 Byzantine faults) are impossible; tests will document such failures to highlight the impossibility result.

## Algorithms Implemented
- **Raft** – for crash/omission/timing failures (leader‑based, easy to understand and debug).
- **PBFT (Practical Byzantine Fault Tolerance)** – for Byzantine scenarios (partial‑synchronous model, widely used in practice).
- **Lamport‑Shostak‑Pease (Oral Messages)** – for Byzantine scenarios (synchronous model, exponential messages; demonstrates theoretical foundation).

## Expected Outcomes
- Correct consensus under all feasible failure configurations.
- Comparative performance metrics (message count, convergence time, memory usage).
- Insights into the practicality of each algorithm for real‑world drone coordination.

## Repository Structure
- `/src` – C++ source files for each algorithm.
- `/topologies` – Mininet Python scripts defining the network topologies.
- `/tests` – Automation scripts to run the scenarios and collect results.
- `/docs` – Final report and analysis.

## Running the Project
1. Install Mininet and dependencies.
2. Compile the C++ executables.
3. Launch a Mininet topology and run the node binaries on each virtual node.
4. Introduce failures (via Mininet’s link manipulation or process killing) and observe consensus.

## Project structure:

/project_root
├── CMakeLists.txt                (Root build file)
├── /proto
│   └── node.proto               (Message & service definitions)
├── /network
│   ├── messaging_interface.hpp   (Abstract interface)
│   ├── grpc_messaging.hpp        (gRPC implementation header)
│   ├── grpc_messaging.cpp        (gRPC implementation source)
│   └── CMakeLists.txt            (Network library build)
├── /src
│   ├── raft_algorithm.hpp        (Will use MessagingLayer*)
│   ├── pbft_algorithm.hpp
│   └── lsp_algorithm.hpp
├── /emulation
│   ├── topology_5.py             (Mininet scripts)
│   └── topology_10.py
└── /logs
    └── (output metric files)