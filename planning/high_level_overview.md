# TillStream: High-Level Overview

## Project Mission
TillStream is a production-grade, multi-tenant retail event streaming platform. It simulates real-time Point-of-Sale (POS) event streams across hundreds of synthetic retail tenants, building a robust ingestion layer around them. The core focus is on distributed systems resilience, schema governance, and full observability.

## Technology Stack
* **Message Broker**: Apache Kafka (KRaft mode)
* **Data Contracts**: Confluent Schema Registry with **Avro**
* **Languages (Polyglot Microservices)**: 
  * **Golang**: Utilized for the Producer layer to leverage Goroutines for highly concurrent, high-throughput event generation across thousands of simulated tenants.
  * **Python**: Utilized for the Consumer layer, Dead-Letter Queue (DLQ) handling, and future ML/AI integrations.
* **Observability**: Prometheus, Grafana, Kafka Exporter
* **Infrastructure**: Docker Compose

## Developer Standards
* **Commit Strategy**: Commit frequently in logical, atomic chunks. 
* **Commit Format**: Strictly adhere to Conventional Commits (e.g., `feat(scope): desc`, `fix(scope): desc`, `chore(scope): desc`).
* **Feature Branches**: Treat Jiras as branches and merge via Pull Requests (even if solo) to maintain a clean `main` branch.

## Implementation Phases
1. **Phase 0 — Environment & Foundations**: Dockerized Kafka infrastructure, Schema Registry, and monitoring stack.
2. **Phase 1 — Producers (Golang)**: High-performance synthetic POS event generator emitting Avro serialized events to core topics.
3. **Phase 2 — Topic & Partition Design**: Tenant-based partitioning strategy designed to handle intentional volume skew and test consumer lag.
4. **Phase 3 — Schema Evolution**: Forcing breaking schema changes and managing forward/backward compatibility.
5. **Phase 4 — Consumers (Python) & Multi-Group**: Isolated consumer groups for distinct business logic, including a robust DLQ pattern for poison messages.
6. **Phase 5 — Failure Injection**: Chaos engineering to test system recovery (broker loss, poison messages).
7. **Phase 6 — Observability**: Advanced dashboards and alerting rules for lag, throughput, and system health.
8. **Phase 7 — The Lakehouse Extension**: Spark Structured Streaming to write Avro data to Apache Iceberg tables stored on MinIO, with a Trino query layer.
9. **Phase 8 — MLOps & Data Quality**: Integration of Great Expectations and Evidently AI for data drift monitoring and schema validation.
10. **Phase 9 — The "God Tier" (Agentic DLQ Resolver)**: A local LLM Agent that autonomously monitors the DLQ, writes Python scripts to fix schema mismatches, and replays clean messages.
