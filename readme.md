# Hermes Broker

Hermes is a resilient, in-memory message broker built with Go and gRPC, designed for high throughput and reliability. It implements Hexagonal Architecture (Ports & Adapters) to ensure decoupling between the core logic, communication protocols, and storage.

## Key Features

- **gRPC Protocol**: High-performance binary communication with Protobuf.
- **In-Memory Engine**: Ultra-low latency message routing using Go Channels and Mutexes.
- **Persistence (WAL)**: Implements a Write-Ahead Log to persist messages on disk, ensuring data recovery in case of crashes.
- **Reliability System**:
  - **Acknowledgments (ACK)**: Ensures messages are processed before removal.
  - **Automatic Retries**: Re-delivers messages if no ACK is received within the timeout window.
  - **Dead Letter Queue (DLQ)**: Automatically moves failed messages to a specific topic (`hermes.dlq`) after max retries.
- **Scalability**:
  - **Pub/Sub (Fan-out)**: Broadcasts messages to all independent subscribers.
  - **Consumer Groups**: Supports Load Balancing (Worker Queue pattern) to distribute processing across multiple instances.

## Architecture

- **Core**: Pure Go logic, isolated from external dependencies (`internal/core`).
- **Primary Adapter**: gRPC Server handling incoming requests (`internal/adapters/primary/grpc`).
- **Secondary Adapters**:
  - **Memory Engine**: Manages channels and concurrent access.
  - **File Persistence**: Handles `hermes.wal` for durability.

## Usage

### 1. Start the Server
The server will start on port `:50051` and recover any state from `hermes.wal`.

```bash
go run cmd/server/main.go



protoc --go_out=. --go-grpc_out=. pb/broker.proto


go get github.com/99designs/gqlgen@v0.17.85
go run github.com/99designs/gqlgen generate