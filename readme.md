# Hermes Broker

Hermes is a high-throughput message broker written in **Go**, built to explore real-world tradeoffs in distributed systems: **latency vs durability**, **throughput vs consistency**, and **simplicity vs control**.

It supports **3 delivery modes** (strong / eventual / in-memory), runs as a **Raft-replicated cluster**, and uses **Pebble** for durable local storage.

---

## Highlights

- **Three delivery modes**
  - **CONSISTENT (strong):** acknowledged only after Raft commit + apply (quorum)
  - **EVENTUAL:** leader writes first, followers replicate asynchronously
  - **PERFORMANCE (weak / RAM):** in-memory pipeline (ultra-low latency, no durability)
- **Distributed cluster (Raft)**
  - Leader election + log replication
  - Followers proxy publish/subscribe/ack to leader when needed
- **Storage**
  - **Message log:** Pebble (`messages.pebble/`)
  - **Raft log:** `raft-pebble` (WAL + KV)
- **Protocols**
  - **gRPC + gRPC streaming** (primary path)
  - HTTP gateway (REST / GraphQL) for convenience and debugging
- **Reliability layer**
  - ACK tracking + retry loop
  - DLQ topic: `hermes.dlq`
  - Consumer groups (offset tracking)

---

## Delivery Modes

| Mode | Publish acknowledgment | Durability | Replication | When to use |
|------|------------------------|-----------|------------|-------------|
| **CONSISTENT** | After Raft commit + apply | ✅ disk | ✅ quorum (sync) | strongest guarantee |
| **EVENTUAL** | Fast ack on leader | ✅ disk (leader first) | ✅ async to followers | best balance |
| **PERFORMANCE** | In-memory delivery pipeline | ❌ (RAM only) | ❌ (RAM only) | max throughput / low latency |

> **Important:** PERFORMANCE mode is not durable. Messages can be lost on restart/crash.

---

## Benchmarks (GKE – e2-medium class)

Benchmarks are executed using `cmd/benchmark/main.go` (scenario `live`), mostly over **gRPC streaming**.

- **CONSISTENT:** ~4k msg/s (strong consistency)
- **EVENTUAL (grpc-stream):** ~47k msg/s
- **EVENTUAL (sustained):** ~9.3k msg/s @ 10k msg/s target for 1h+
- Disk footprint after sustained run: ~**1.1–1.2 GB per node** (replicated)

Full details: see [`benchmark.md`](./benchmark.md)

---

## Run Locally (Docker Compose)

> `*.hermes-internal` resolves only **inside containers**. From your host machine, use `localhost` ports.

Start the cluster:

```bash
docker compose up --build
````

Ports (host → container):

* hermes-1 → `localhost:50051` (HTTP `localhost:8080`)
* hermes-2 → `localhost:50052` (HTTP `localhost:8081`)
* hermes-3 → `localhost:50053` (HTTP `localhost:8082`)


### Run benchmark from host (example)

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

---

## Run on GKE (Cluster)

Hermes is typically deployed as a **StatefulSet** (1 pod per node), using a **headless service** for stable DNS.

See manifests under `cloud-build/`:

* `statefulset.yaml`
* `internal-svc.yaml`
* `client-svc.yaml`
* (optional) `bench-runner-isolated.yaml`

---

## Development

Run server:

```bash
go run cmd/server/main.go
```

Generate Protobuf:

```bash
protoc --go_out=. --go-grpc_out=. proto/broker.proto
```

GraphQL (if using gqlgen in your setup):

```bash
go get github.com/99designs/gqlgen@v0.17.85
go run github.com/99designs/gqlgen generate
```