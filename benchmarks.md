# Hermes Broker: Performance & Reliability Benchmarks

This document details the stress tests performed on **Hermes Broker** to validate its processing capacity, resilience in failure scenarios, and efficiency of its communication protocols (**gRPC, REST, and GraphQL**).

---

## Results: Comparison by Protocol

The tests below were performed with **60 simultaneous workers** for **30 seconds** in a stable 3-node cluster.

| Protocol | Throughput (msg/sec) | Efficiency | Technical Observation |
| --- | --- | --- | --- |
| **gRPC** | **977.32** | **100%** | High performance via **Protobuf** and HTTP/2. |
| **REST** | **842.68** | ~86% | Serialization overhead **JSON** over HTTP/1.1. |
| **GraphQL** | **830.43** | ~85% | Similar to REST, with extra schema parsing cost. |
| **ALL** | **909.53** | ~93% | Mixed load testing all adapters simultaneously. |

---

## How to Run the Benchmarks

The benchmark executable is located at `cmd/benchmark/main.go`. It allows configuring the stress level, duration, and system behavior.

### Basic Command

```bash
go run cmd/benchmark/main.go -workers 60 -duration 30s -protocol=grpc -scenario=live
```

### Available Flags

* `-workers`: Number of goroutines firing messages (default: 60).
* `-duration`: Total sending time (e.g., `15s`, `1m`).
* `-protocol`: Choose between `grpc`, `rest`, `graphql`, or `all`.
* `-scenario`: Defines the test behavior (see below).
* `-grpc-targets`: List of gRPC cluster IPs/Ports (csv).
* `-http-targets`: List of HTTP cluster IPs/Ports (csv).

---

## Test Scenarios

The benchmark doesn't just test speed, but also the **robustness** of the hybrid engine:

| Scenario | Description | What does it validate? |
| --- | --- | --- |
| `live` | Subscriber consumes in real-time via RAM. | **Memory Bypass** and minimum latency. |
| `slow` | Artificially slow subscriber (2ms delay). | **Backpressure** and disk persistence (BoltDB). |
| `crash` | Subscriber dies halfway through and resurfaces. | **Consumer Groups** and Offset persistence. |
| `isolation` | Messages sent to Topic A and B simultaneously. | **Routing** and logical data isolation. |
| `disk` | Subscriber only starts after publications end. | Complete **Recovery** from physical storage. |

---

## Resilience Analysis (High Availability)

During the `slow` scenario, we performed the **Leader Kill** test (killing the leader node while the backlog is being processed).

### Technical Observations:

* **Transparent Failover:** The Auditor (Subscriber) detected the gRPC stream failure and reconnected to a follower automatically.
* **Raft Consistency:** The new leader assumed the offset of the `ha-slow-group` group and continued delivery from the last replicated point.
* **At-Least-Once Guarantee:** In some failure cases, the total `Received` messages may be slightly higher than those `Published`. This proves that the system prefers to resend a message rather than lose data during the election of a new leader.

> [!IMPORTANT]
> **Idempotency:** Since Hermes guarantees at-least-once delivery, it is recommended that consumers handle duplicate messages based on the `message_id`.

---

## Conclusions

1. **Binary Efficiency:** gRPC proved to be ~15% more efficient than text-based protocols (JSON), being the ideal choice for inter-service communication.
2. **Hybrid Engine:** BoltDB integrated with Raft allows the Broker to accept loads far exceeding consumer processing capacity, safely storing the excess.
3. **Smart Proxy:** The implementation of the internal proxy in the server ensures that the client doesn't need to know the cluster topology, being able to hit any node to publish data.

---

## Post-Optimization Results (Hybrid Engine v2)

After implementing **Nagle's Algorithm (Batching)** in Raft and **Object Pooling (sync.Pool)** in the Service Layer, the results were updated:

| Protocol | Workers | Throughput (msg/sec) | Status |
| :--- | :--- | :--- | :--- |
| gRPC (Baseline) | 60 | 977.32 | Stable |
| **gRPC (Optimized)** | 60 | **1,352.56** | High Performance |
| **gRPC (Stress)** | 150 | **1,604.96** | Hardware Limit |

### Applied Technical Improvements
1. **Raft Log Batching:** Grouping of up to 500 messages per write operation.
2. **Memory Recycling:** Use of `sync.Pool` to reduce `domain.Message` allocations by 90%.
3. **Smart Proxy:** Automatic failover and traffic redirection between Followers and Leader.