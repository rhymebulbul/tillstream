# Design Document 03: Producer/Consumer Patterns & Resilience Engineering

**Author:** Staff Data Engineer
**Status:** Approved
**Last Updated:** August 2026

## 1. Executive Summary
This document outlines the distributed systems patterns utilized in the TillStream ingestion and consumption layers to ensure exactly-once semantics (EOS), mitigate data skew ("noisy neighbor" problems), and handle downstream infrastructure degradation via Dead Letter Queues (DLQs).

## 2. Producer Strategy & Data Skew Management
In B2B SaaS platforms, traffic follows a Pareto distribution (e.g., 20% of tenants generate 80% of the data). A naive Round-Robin partitioning strategy would balance load but destroy event ordering.

### 2.1 Deterministic Hashing
*   Messages are strictly keyed by `tenant_id`. 
*   Kafka uses the `murmur2` algorithm to hash the key: `hash("TENANT_FLAGSHIP_1") % num_partitions`.
*   **Guarantee:** All events for a specific tenant are routed to the exact same partition, guaranteeing chronological ordering necessary for accurate state machine transitions (e.g., wallet balance updates).

### 2.2 Mitigating Hot Partitions (The Salted Key Pattern)
If `TENANT_FLAGSHIP_1` exceeds the throughput capacity of a single partition (e.g., > 10MB/s), it will cause a "Hot Partition", throttling the cluster.
*   **Future Mitigation:** If a tenant is flagged as "Flagship", the Producer appends a random salt suffix (e.g., `TENANT_FLAGSHIP_1_A`, `TENANT_FLAGSHIP_1_B`) splitting the tenant's load across a pre-defined number of partitions. The downstream consumer must then implement a K-Way Merge to reconstruct global ordering.

## 3. Producer Resiliency (Idempotence & Buffering)
*   **Exactly-Once Semantics (EOS):** The Go Producer is configured with `enable.idempotence=true` and `acks=all`. Kafka assigns a Producer ID (PID) and sequence number to each message. If a network timeout causes the producer to retry, the broker deduplicates the message based on the sequence number.
*   **Chaos Engineering Response:** During simulated broker failure (INC-001), the producer relies on its internal `queue.buffering.max.messages` and `message.timeout.ms`. It buffers payloads in memory rather than failing the upstream HTTP request, flushing them instantly when the broker leader is re-elected.

## 4. Consumer DLQ (Dead Letter Queue) Architecture
Head-of-Line blocking occurs when a consumer cannot process a message (e.g., a downstream database is offline) and halts the entire partition.

### 4.1 Transient vs Poison Failures
*   **Poison Pills:** Un-parseable bytes or schema violations.
*   **Transient Failures:** Database timeouts, API rate limits (HTTP 429).

### 4.2 The DLQ Implementation
Instead of crashing, the Python consumer catches the exception.
1.  It wraps the original un-deserialized payload, the original Kafka Key, and the Exception Stack Trace into a new envelope.
2.  It routes this envelope to the `orders-dlq` topic.
3.  It commits the offset for the failed message on the main topic and continues processing the stream.
4.  **DLQ Resolver Agent (Phase 9):** A separate autonomous process monitors the DLQ, parses the stack trace, and attempts automated remediation via LLM reasoning.
