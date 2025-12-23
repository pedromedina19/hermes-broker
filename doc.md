# 📘 Hermes Broker Integration Guide

Hermes is a high-throughput, polyglot message broker designed to decouple your microservices. It supports **gRPC**, **REST**,**GraphQL**.

## ⚡ Connection Overview

| Protocol | Port |
|----------|------|
| **gRPC** | `:50051` |
| **REST** | `:8080` |
| **GraphQL** | `:8080` |

---

## Option 1: gRPC (Go, Python, Java, etc.)

This is the most performant way to communicate with Hermes.

### 1. Get the Proto Definition
You need the `.proto` file located at `proto/broker.proto`.

### 2. Generate Client Code
Use `protoc` to generate the client for your language.
Example for Go:
```bash
protoc --go_out=. --go_grpc_out=. proto/broker.proto

```

### 3. Usage Example (Go)

```go
package main

import (
    "context"
    "log"
    "time"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "path/to/your/project/pb" // Import generated proto
)

func main() {
    // 1. Connect
    conn, _ := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
    defer conn.Close()
    client := pb.NewBrokerServiceClient(conn)

    // 2. Publish (Fire and Forget)
    ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
    defer cancel()
    
    _, err := client.Publish(ctx, &pb.PublishRequest{
        Topic:   "orders",
        Payload: []byte(`{"order_id": 123}`),
    })
    
    // 3. Subscribe
    stream, _ := client.Subscribe(context.Background())
    
    // Handshake
    stream.Send(&pb.SubscribeRequest{
        Action:  "SUBSCRIBE",
        Topic:   "orders",
        GroupId: "order-processors", // Optional: For Load Balancing
    })

    // Loop
    for {
        msg, _ := stream.Recv()
        log.Printf("Received: %s", msg.Payload)
        
        // Send ACK
        stream.Send(&pb.SubscribeRequest{
            Action:       "ACK",
            AckMessageId: msg.Id,
        })
    }
}

```

---

## Option 2: REST API

Ideal for simple integrations or webhooks where installing gRPC libraries is not possible.

**Endpoint:** `POST http://localhost:8080/publish`

### Example (cURL)

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
        "topic": "notifications", 
        "payload": "User signed up!"
      }'

```

### Response

```json
{
  "status": "published"
}

```

---

## ⚛️ Option 3: GraphQL

Perfect for Front-end applications needing real-time updates via WebSockets.

**Endpoint:** `http://localhost:8080/query` (Supports Playground)

### Publishing (Mutation)

```graphql
mutation {
  publish(topic: "chat-room", payload: "Hello World") {
    success
  }
}

```

### Real-Time Listening (Subscription)

Hermes uses WebSockets for subscriptions.

```graphql
subscription {
  subscribe(topic: "chat-room", groupId: "mobile-app") {
    id
    payload
    timestamp
  }
}

```

---

## Scalability: Consumer Groups

Hermes supports two consumption patterns:

1. **Fan-Out (Broadcast):**
* If you connect **without** a `groupID`, every subscriber receives a copy of the message.
* *Use case:* Notifications, Logs, Real-time dashboards.


2. **Worker Queue (Load Balancing):**
* If multiple clients connect with the **same** `groupID`, Hermes uses **Round-Robin** to distribute messages.
* Only one worker receives each message.
* *Use case:* Heavy background processing (e.g., resizing images, processing payments).

---

## Best Practices

1. **Timeouts:** Always set a timeout (Context Deadline) when publishing via gRPC to avoid hanging threads if the broker is under heavy load.
2. **Acknowledgments:** Hermes waits for an ACK. If your worker crashes before sending an ACK, Hermes will redeliver the message after 5 seconds. Ensure your consumers are **Idempotent** (can handle receiving the same message twice).
3. **Dead Letter Queue:** If a message fails 3 times, it is moved to `hermes.dlq`. Monitor this topic for failures.

```