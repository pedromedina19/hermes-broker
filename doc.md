# Hermes Broker Integration Guide

Hermes is a high-throughput, polyglot message broker designed for distributed resilience. Built on top of the **Raft Consensus Protocol**, it ensures data consistency across a cluster while providing ultra-fast delivery via **Memory Bypass**.

## Connection Overview

Hermes operates as a cluster. You can connect to **any node**; if a node is not the Leader, it will transparently proxy your request to the correct node.

| Protocol | Port | Description |
| --- | --- | --- |
| **gRPC** | `:50051` | Recommended for high-performance microservices. |
| **REST** | `:8080` | Simple HTTP integration and health checks. |
| **GraphQL** | `:8080` | Real-time subscriptions for web/mobile apps. |

---

## Architecture: The Hybrid Engine

Hermes uses a dual-path delivery system:

1. **Hot Path (Memory Bypass):** Messages are delivered instantly from RAM to active subscribers.
2. **Cold Path (Disk Recovery):** If a subscriber is slow or reconnects, Hermes streams data directly from the persistent **BoltDB** storage.

---

## Option 1: gRPC (High Performance)

### 1. Proto Definition

Location: `proto/broker.proto`.

### 2. Usage Example (Go)

The gRPC client should implement a retry logic to handle **Leader Election** windows (usually < 2s).

```go
// Example of a resilient subscriber
func subscribeWithRetry(client pb.BrokerServiceClient) {
    for {
        stream, _ := client.Subscribe(context.Background())
        
        // SUBSCRIBE handshake
        stream.Send(&pb.SubscribeRequest{
            Action:  "SUBSCRIBE",
            Topic:   "payments",
            GroupId: "billing-service", // Offset is persisted for this group
        })

        for {
            msg, err := stream.Recv()
            if err != nil { break } // Reconnect on error
            
            process(msg)
            
            // ACK ensures the message won't be redelivered
            stream.Send(&pb.SubscribeRequest{
                Action: "ACK", 
                AckMessageId: msg.Id,
            })
        }
        time.Sleep(1 * time.Second)
    }
}

```

---

## Option 2: REST API

Hermes supports high-speed publishing and real-time streaming via standard HTTP.

### Publishing
**Endpoint:** `POST /publish`
```bash
curl -X POST http://localhost:8080/publish \
  -d '{"topic": "logs", "payload": "System start"}'

```

### Real-Time Subscription (SSE)

Hermes uses **Server-Sent Events** for HTTP streaming. This allows any HTTP client (like a browser or `curl`) to receive a continuous stream of messages.

**Endpoint:** `GET /subscribe?topic=<name>&group=<optional>`

**Example with cURL:**

```bash
curl -N "http://localhost:8080/subscribe?topic=orders&group=rest-consumer"

```

**Example with JavaScript (Browser):**

```javascript
const eventSource = new EventSource("http://localhost:8080/subscribe?topic=orders");

eventSource.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    console.log("New Message:", msg.payload);
};

```
---

## Option 3: GraphQL (Web/Real-time)

Você tem razão, foi uma falha minha não incluir o exemplo de **Mutation** na seção de GraphQL do guia. Como o GraphQL é excelente para integrações web, o `publish` via Mutation é essencial.

Aqui está o trecho que completa a **Opção 3** do seu `doc.md`:

---

## Option 3: GraphQL (Web/Real-time)

Perfect for frontend applications or platforms that prefer a single endpoint for all operations. Hermes provides both publishing and real-time streaming via GraphQL.

**Endpoint:** `http://localhost:8080/query`

### 1. Publishing (Mutation)

To send a message via GraphQL, use the `publish` mutation.

```graphql
mutation {
  publish(topic: "orders", payload: "{\"id\": 1, \"status\": \"paid\"}") {
    success
  }
}

```

### 2. Real-Time Listening (Subscription)

Hermes uses **WebSockets** for GraphQL subscriptions, allowing browsers to receive messages instantly.

```graphql
subscription {
  subscribe(topic: "orders", groupId: "dashboard-client") {
    id
    payload
    timestamp
  }
}

```
---

## Scalability & Consumer Groups

Hermes manages message distribution based on the `groupId`:

| Mode | `groupId` | Behavior | Use Case |
| --- | --- | --- | --- |
| **Broadcast** | *Empty* | Every instance gets every message. | Real-time UI, Cache invalidation. |
| **Persistent** | `name` | Hermes remembers the position (offset). If the service restarts, it continues from where it left off. | Database syncing, Billing. |
| **Replay** | `__REPLAY__` | Reads the entire history from disk. | Auditing, New service data-fill. |

---

## High Availability (HA)

Hermes is a **CP System** (Consistent & Partition Tolerant).

* **Leader Election:** If the leader fails, a new one is elected automatically.
* **Transparent Proxy:** If you `Publish` to a Follower, it forwards the message to the Leader internally.
* **Data Integrity:** No message is acknowledged to the publisher until it is safely replicated to a majority of nodes.

---

## Best Practices

1. **Idempotency:** In distributed systems, "at-least-once" delivery is the norm. Ensure your consumer can handle the same `message_id` twice.
2. **Ack Pipeline:** Send ACKs as soon as processing is finished to keep the `Pending` buffer low.
3. **Dead Letter Queue:** Messages failing more than 3 times are moved to `hermes.dlq`.
