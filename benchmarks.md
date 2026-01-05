# Hermes Benchmarks

This document tracks **Hermes Broker** performance across the three delivery modes supported by the system:

* **CONSISTENT (strong consistency)**
* **EVENTUAL (eventual consistency)**
* **PERFORMANCE (weak consistency / in-memory)**

Benchmarks are executed using `cmd/benchmark/main.go` (scenario `live`), primarily over **gRPC / gRPC streaming**.

---

## Test Environment (GKE – Production-like)

* **Cluster:** GKE
* **Nodes:** 3 nodes (one Hermes pod per node)
* **Machine Type:** e2-medium class (2 vCPU)
* **Resources available per node:** ~2 vCPU / ~2 GB RAM (Hermes workload)
* **Storage:**
  * **Message log:** Pebble (message persistence)
  * **Raft log:** raft-pebble (WAL + KV store)

---

## Delivery Modes & Guarantees

| Mode            | What happens on publish                                             | Persistence           | Replication                     | Tradeoff                                  |
| --------------- | ------------------------------------------------------------------- | --------------------- | ------------------------------- | ----------------------------------------- |
| **CONSISTENT**  | publish is acknowledged after the Raft log is committed and applied | ✅ disk                | ✅ synchronous (commit quorum)   | Lowest throughput, strongest guarantee    |
| **EVENTUAL**    | publish is acknowledged fast; replication happens asynchronously    | ✅ disk (leader first) | ✅ async to followers            | High throughput with eventual convergence |
| **PERFORMANCE** | publish is routed to in-memory delivery pipeline                    | ❌ (RAM only)          | ❌ (RAM only)                    | Highest throughput, no durability         |

> **Important:** PERFORMANCE mode is designed for ultra-low latency delivery. Messages are not durable and can be lost on restart/crash.

---

## Benchmark Summary

### Strong consistency (CONSISTENT)

* **Workers:** 120
* **Duration:** 30s
* **Published:** 133,128
* **Throughput:** **~4,284 msg/s**

### Eventual consistency (EVENTUAL, gRPC stream)

* **Workers:** 1
* **Duration:** 30s
* **Published/Received:** 1,477,174
* **Throughput:** **~47,594 msg/s**

### Eventual consistency (EVENTUAL, sustained – rate limited)

* **Workers:** 1
* **Duration:** 3,800s (1h03m20s)
* **Rate:** 10,000 msg/s (target)
* **Published/Received:** 35,543,300
* **Throughput:** **~9,351 msg/s** (stable over 1+ hour)

### Disk footprint (after sustained run)

* **messages.pebble size:** ~**1.1–1.2 GB per node**, with consistent replication across the cluster.

---

## How to Reproduce (GKE)

### 1) Create a bench-runner pod

Option A (recommended):
```bash
kubectl apply -f ./cloud-build/bench-runner-isolated.yaml
````

Option B (quick ad-hoc pod):

```bash
kubectl run bench-runner --image=golang:1.25 --restart=Never -- sleep infinity
```

### 2) Copy the repository into the pod

From the repo root:

```bash
kubectl cp . bench-runner:/app
```

### 3) Run benchmarks

**PERFORMANCE (gRPC unary / default protocol)**

```bash
kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=60 \
  -duration=30s \
  -mode=performance \
  -grpc-targets='hermes-0.hermes-internal:50051,hermes-1.hermes-internal:50051,hermes-2.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"
```

**EVENTUAL (gRPC unary / default protocol)**

```bash
kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=60 \
  -duration=30s \
  -mode=eventual \
  -grpc-targets='hermes-0.hermes-internal:50051,hermes-1.hermes-internal:50051,hermes-2.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"
```

**CONSISTENT (gRPC unary / default protocol)**

```bash
kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=60 \
  -duration=30s \
  -mode=consistent \
  -grpc-targets='hermes-0.hermes-internal:50051,hermes-1.hermes-internal:50051,hermes-2.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"
```

**PERFORMANCE (gRPC stream)**

```bash
kubectl exec -it bench-runner -- bash -c "cd /app && \
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=100 \
  -duration=30s \
  -mode=performance \
  -protocol=grpc-stream \
  -grpc-targets='hermes-0.hermes-internal:50051' \
  -sub-target='hermes-0.hermes-internal:50051'"
```

**EVENTUAL sustained (rate limited, gRPC stream)**

```bash
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
```

### 4) Cleanup

```bash
kubectl delete pod bench-runner
```

---

## How to Reproduce (Local – Docker Compose)

When running on Docker Compose, `*.hermes-internal` resolves **only inside containers**.
From your OS host, use `localhost` ports (based on your compose mapping):

* hermes-1 → `localhost:50051` (HTTP `localhost:8080`)
* hermes-2 → `localhost:50052` (HTTP `localhost:8081`)
* hermes-3 → `localhost:50053` (HTTP `localhost:8082`)

Example (host machine):

```bash
go run cmd/benchmark/main.go \
  -scenario=live \
  -workers=1 \
  -duration=30s \
  -mode=eventual \
  -protocol=grpc-stream \
  -grpc-targets='localhost:50051' \
  -sub-target='localhost:50051'
```

```