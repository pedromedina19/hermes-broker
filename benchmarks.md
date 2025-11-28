# Performance Benchmarks

This document outlines the performance metrics collected during the ingestion of **100,000 user records** via CSV/XLSX.

## Test Environment

- **Hardware:** Apple M2 (8 Cores)
- **Database:** PostgreSQL (Docker container)
- **Dataset:** 100,000 rows (Name, Email, Phone, Document)

## Execution Scenarios

We tested two distinct configurations to evaluate throughput and resource management.

### Scenario A: Unconstrained Resources (High Performance)
Leveraging the full power of the host machine without Docker CPU limits.

- **Workers:** 80 concurrent workers
- **Broker Buffer:** 80,000 messages
- **Total Time:** ~30 seconds
- **Throughput:** ~3,333 inserts/second

### Scenario B: Production Simulation (Limited Resources)
Simulating a cloud environment (e.g., GCP e2-medium) by limiting the Database container to **1 vCPU**.

- **Workers:** 30 concurrent workers (Optimized)
- **Broker Buffer:** 20,000 messages
- **Total Time:** ~41 seconds
- **Throughput:** ~2,439 inserts/second

*Note: Increasing workers to 80 in this limited scenario degraded performance to 55 seconds due to CPU thrashing and context switching.*

## Resource Consumption

The following table represents the resource usage of each microservice during the peak load of processing 100,000 records.

| Service | Component Role | Memory Usage (Alloc) | Goroutines (Peak) | Performance Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **Janus** | Gateway / Validation | ~29 MiB | 18 | Stable memory usage due to stream processing. |
| **Hermes** | Message Broker | ~18 MiB (Peak) | 21 | High allocation turnover due to message passing. |
| **Hephaestus** | Worker / DB Writer | ~6 MiB | 73 | Extremely low memory footprint due to batch flushing. |

## Key Findings

1.  **Memory Efficiency:** The entire architecture (3 services) consumed less than **100 MiB** of actual RAM (Alloc) to process the dataset, proving the efficiency of Go's memory management and garbage collector.
2.  **Backpressure:** The system successfully handled backpressure. When the Database reached 100% CPU usage (in Scenario B), the Broker buffer filled up, causing the Gateway to slow down reading, preventing data loss.
3.  **Concurrency Limit:** More workers do not always equal higher speed. In a 1 vCPU database environment, 30 workers performed 25% faster than 80 workers by reducing lock contention and context switching.