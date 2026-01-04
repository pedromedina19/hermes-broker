# Hermes Broker: Performance & Reliability Benchmarks

This document details the stress tests performed on **Hermes Broker** to validate its processing capacity, resilience in failure scenarios, and efficiency of its communication protocols (**gRPC, REST, and GraphQL**).

---

## Results: Comparison by Protocol

The tests below were performed with **60 simultaneous workers** for **30 seconds** in a stable 3-node cluster (Docker local environment).

| Protocol | Throughput (msg/sec) | Efficiency | Technical Observation |
| --- | --- | --- | --- |
| **gRPC** | **1,438.29** | **100%** | Peak performance via **Protobuf** and connection pooling. |
| **REST** | **842.68** | ~58% | Serialization overhead (**JSON**) and HTTP/1.1 limitations. |
| **GraphQL** | **830.43** | ~57% | Similar to REST, with extra schema validation cost. |
| **ALL** | **909.53** | ~63% | Mixed load testing all adapters simultaneously. |

---

## How to Run the Benchmarks

The benchmark executable is located at `cmd/benchmark/main.go`. It allows configuring the stress level, duration, cluster topology, and specific targets for publishers and subscribers.

### Basic Command

```bash
# Standard test: 60 workers, 30 seconds, using gRPC
go run cmd/benchmark/main.go -workers 60 -duration 30s -protocol=grpc -scenario=live

```

### Advanced Cluster Testing (Follower Consumption)

To test Raft replication by publishing to the cluster and consuming from a specific follower:

```bash
go run cmd/benchmark/main.go \
  -workers 60 \
  -duration 30s \
  -protocol=grpc \
  -grpc-targets=localhost:50051,localhost:50052,localhost:50053 \
  -sub-target=localhost:50053

```

### Available Flags

* `-workers`: Number of goroutines firing messages (default: 60).
* `-duration`: Total sending time (e.g., `30s`, `2m`).
* `-protocol`: Choose between `grpc`, `rest`, `graphql`, or `all`.
* `-scenario`: Defines the test behavior (`live`, `slow`, `crash`, `isolation`, `disk`).
* `-grpc-targets`: List of gRPC cluster IPs/Ports (csv).
* `-sub-target`: Specific gRPC node for the Subscriber/Auditor to consume from.
* `-http-targets`: List of HTTP cluster IPs/Ports (csv).

---

## Test Scenarios

| Scenario | Description | What does it validate? |
| --- | --- | --- |
| `live` | Subscriber consumes in real-time via RAM. | **Memory Bypass** and minimum E2E latency. |
| `slow` | Artificially slow subscriber (2ms delay). | **Backpressure** and disk persistence (BoltDB). |
| `crash` | Subscriber dies halfway through and resurfaces. | **Consumer Groups** and Offset persistence. |
| `isolation` | Messages sent to Topic A and B simultaneously. | **Routing** and logical data isolation. |
| `disk` | Subscriber only starts after publications end. | Complete **Recovery** from physical storage. |

---

## Internal Monitoring (Production Ready)

Hermes Broker includes a built-in metrics system using atomic counters for zero-overhead monitoring. You can check the health of any node via a simple HTTP request.

### Monitoring Command

```bash
curl -s http://localhost:8080/status | jq

```

### Metrics Definitions

* **is_leader**: Boolean indicating if the node is currently the Raft Leader.
* **messages_published**: Total messages successfully committed to the Raft log.
* **messages_consumed**: Total messages delivered to active subscribers.
* **memory_allocated_mb**: Current heap allocation (useful for detecting leaks).
* **goroutines**: Number of active threads (useful for monitoring scale).

---

## Resilience Analysis (High Availability)

During the `slow` scenario, we performed the **Leader Kill** test (killing the leader node while the backlog is being processed).

### Technical Observations:

* **Transparent Failover:** The Auditor (Subscriber) detects the gRPC stream failure and reconnected to a follower automatically.
* **Raft Consistency:** The new leader assumes the exact offset of the `ha-slow-group` and continues delivery.
* **At-Least-Once Guarantee:** In failure cases, total `Received` may be slightly higher than `Published`. This proves the system prioritizes data safety (no loss) over strict exactly-once delivery during leader elections.

---

## Post-Optimization Results (Hybrid Engine v2)

After implementing **Connection Pooling** in the Proxy layer and **Non-blocking Notifications** in the Batcher, the performance reached a new ceiling:

| Configuration | Workers | Throughput (msg/sec) | Status |
| --- | --- | --- | --- |
| gRPC (Baseline) | 60 | 977.32 | Stable |
| **gRPC (Optimized)** | 60 | **1,438.29** | High Performance |
| **gRPC (Maximum Load)** | 150 | **1,720.40** | Network/Disk Bound |

### Key Improvements

1. **gRPC Connection Reuse:** The Internal Proxy now maintains persistent connections to the Leader, eliminating TCP handshake overhead.
2. **Atomic Metrics:** Real-time stats without using expensive Mutexes.
3. **Dedicated Sub-Targeting:** Capability to verify replication lag by consuming from any node in the cluster.


how to run in cluster

kubectl apply -f ./cloud-build/bench-runner-isolated.yaml
ou
kubectl run bench-runner --image=golang:1.25 --restart=Never -- sleep infinity

kubectl cp . bench-runner:/app

kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=60 \
  -duration=30s \
  -mode=performance \
  -grpc-targets='hermes-0.hermes-internal:50051,hermes-1.hermes-internal:50051,hermes-2.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"

kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=60 \
  -duration=30s \
  -mode=eventual \
  -grpc-targets='hermes-0.hermes-internal:50051,hermes-1.hermes-internal:50051,hermes-2.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"

kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=60 \
  -duration=30s \
  -mode=consistent \
  -grpc-targets='hermes-0.hermes-internal:50051,hermes-1.hermes-internal:50051,hermes-2.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"

kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=100 \
  -duration=30s \
  -mode=performance \
  -protocol=grpc-stream \
  -grpc-targets='hermes-0.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"

kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=1 \
  -duration=3800s \
  -mode=eventual \
  -rate=10000 \
  -protocol=grpc-stream \
  -grpc-targets='hermes-0.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"



kubectl delete pod bench-runner