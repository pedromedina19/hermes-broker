# Hermes Broker

Hermes is a high-performance, in-memory message broker built with gRPC. It acts as the intermediary between the API Gateway and the Workers, facilitating decoupling and load management.

## Responsibilities

- Provides a gRPC server for Publishing and Subscribing events.
- Manages high-throughput data streams using Go channels.
- Implements backpressure mechanisms to handle load spikes, blocking producers when consumers are slow to prevent data loss.
- Automatically adjusts buffer sizes based on available CPU/Hardware resources.

## Usage

To start the broker server:

```bash
go run cmd/server/main.go