# Design Doc 03: Producer-Consumer Patterns & Resilience

## Motivation
A data pipeline is only as reliable as its ability to handle failure and traffic spikes. TillStream implements specific patterns to ensure zero data loss during infrastructure outages and deterministic routing for high-volume tenants.

## 1. Partitioning & Data Skew
In B2B platforms, traffic is rarely distributed evenly. Often, a few "Flagship" tenants generate 80% of the volume.
*   **Tenant-Based Keying:** The Go Producer hashes the `tenant_id` and uses it as the Kafka Message Key. 
*   **Deterministic Routing:** This guarantees that all events for a specific tenant are routed to the *same* Kafka partition.
*   **Ordering Guarantees:** By ensuring a tenant's data is isolated to a single partition, we guarantee strict chronological ordering for that tenant's events, which is critical for transactional state machines (e.g., calculating loyalty points).

## 2. The Dead Letter Queue (DLQ) Pattern
Downstream databases are inherently flaky. If the Python Consumer encounters a database timeout, crashing the consumer would halt the entire pipeline (Head-of-Line blocking).
*   **Non-Blocking Retries:** When TillStream simulates a database timeout (triggered intentionally via chaos engineering), the consumer catches the error.
*   **Quarantine Routing:** The consumer extracts the raw, un-deserialized payload and forwards it to an `orders-dlq` topic.
*   **Resumption:** The consumer immediately resumes processing the main topic. A secondary, isolated microservice can later re-process the DLQ at its own pace.

## 3. Infrastructure Resilience (Chaos Engineering)
TillStream is resilient against total broker failure.
*   **Producer Buffering:** If the Kafka Broker goes offline, the Go producer automatically buffers incoming events in memory, preventing upstream API timeouts.
*   **Consumer Offsets:** The Python consumer tracks its progress using committed offsets. If the cluster crashes, the consumer halts; upon revival, it queries Kafka for its last committed offset and resumes processing exactly where it left off, achieving At-Least-Once delivery semantics.
