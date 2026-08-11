# Proposal 2: TillStream (Multi-Tenant Retail Event Streaming Platform)

## Overview
TillStream is a production-grade, Kafka-first data platform engineering project. It simulates POS event streams (orders, payments, refunds, inventory adjustments) across hundreds of synthetic retail tenants/sites, and builds a robust ingestion layer around them. 

The core focus is on **distributed systems resilience**: partitioning strategy, schema governance, multi-consumer-group architecture, fault injection and recovery, and full observability.

## Why This Proposal? (The FAANG / Top-Tier Resume Strategy)
This project is engineered to definitively prove Senior Data Engineer / Data Platform Engineer capabilities.
* **Resume-Defensible:** Every bullet point maps to something demoable live in an interview, not a tutorial completion badge.
* **Direct Experience Translation:** It perfectly translates real-world, closed-source production experience (e.g., Redcat MariaDB CDC → Iceberg) into open-source, interview-visible proof.
* **Focuses on Hard Problems:** Prioritizes failure modes, consumer lag, poison messages, and schema evolution over basic "happy path" data movement.

---

## Roadmap & Implementation Phases

### Phase 0 — Environment & Foundations
* **Tech:** Docker Compose skeleton (Kafka KRaft mode, Schema Registry, Kafka Connect, Prometheus, Grafana, Kafka Exporter).
* **Repo Structure:** `/producers`, `/consumers`, `/infra`, `/docs`, `/tests`.
* **Deliverable:** A `docker compose up` command that brings up a healthy single-broker Kafka cluster with Schema Registry and Grafana. An ongoing Decision Log (`/docs/decisions.md`) for architectural choices.

### Phase 1 — Producers & Realistic Event Generation
* **Implementation:** Python/Go synthetic POS event generator emitting to 4 topics (orders, payments, refunds, inventory-adjustments).
* **Key Feature:** Deliberate volume skew across tenants (some sites 10x others) to test partitioning later. Idempotent producer configs enabled. Avro/Protobuf schemas registered (no raw JSON).
* **Resume Skill:** Producer reliability config, schema-first event design.

### Phase 2 — Topic & Partition Design
* **Implementation:** Partition by tenant/site ID.
* **Key Feature:** Deliberately under-provision partition counts, generate load, observe consumer lag in Grafana, and reconfigure to show recovery. Implement per-topic retention policies.
* **Resume Skill:** Partition strategy under real skew, operational troubleshooting.

### Phase 3 — Schema Evolution
* **Implementation:** Force a breaking schema change (e.g., new required field) on a live topic.
* **Key Feature:** Handle backward/forward compatibility via Schema Registry. Document intentional failure modes if compatibility is set incorrectly.
* **Resume Skill:** Schema governance in a multi-producer/consumer system.

### Phase 4 — Consumers & Multi-Group Architecture
* **Implementation:** 
  * Consumer Group A: Placeholder lakehouse writer.
  * Consumer Group B: Rule-based fraud/anomaly detector.
* **Key Feature:** Dead-letter queue (DLQ) topic + handler for malformed events. Replay tool to reprocess DLQ messages.
* **Resume Skill:** Consumer group isolation, poison-message handling, replay mechanics.

### Phase 5 — Failure Injection & Recovery (The Interview Closer)
* **Implementation:** Self-induced incidents.
* **Incidents:** 
  1. Kill broker mid-stream (capture rebalancing). 
  2. Inject poison message (confirm DLQ routing). 
  3. Replay from specific offset.
* **Deliverable:** Short "postmortem" docs with timelines, causes, and fixes.
* **Resume Skill:** Production incident response, demonstrated visually.

### Phase 6 — Observability
* **Implementation:** Kafka Exporter → Prometheus → Grafana.
* **Key Feature:** Dashboards for consumer lag, throughput, partition skew. Alert rules for lag thresholds.
* **Deliverable:** Saved Grafana dashboard JSON and screenshots.

---

## Future Scope

### Phase 7+ — Lakehouse Extension
* Replace placeholder sink with Spark Structured Streaming → Apache Iceberg on MinIO.
* Trino query layer on top of Iceberg.
* Airflow orchestration for batch jobs.
* dbt transformations downstream.
* Data quality checks (Great Expectations).

### Phase 8 — "The God Tier" (AI + Infrastructure Hybrid)
* **Agentic DLQ Resolver:** Introduce a local LLM Agent that monitors the DLQ. When a poison message arrives due to a schema mismatch, the Agent autonomously reads the Schema Registry, writes a Python script to fix the payload schema, executes it in a sandbox, and replays the clean message back into the main topic. (Combines traditional Data Platform Engineering with cutting-edge GenAI/Agentic automation).

---

## Targeted Resume Bullet (Draft)
> *"Built TillStream, a multi-tenant event streaming platform processing synthetic POS data across N simulated retail sites; designed topic/partition strategy handling tenant volume skew, implemented schema governance with breaking-change compatibility testing, and built failure-injection testing (broker loss, poison messages, consumer rebalance) with Prometheus/Grafana observability."*

---

## MLOps Integration (Optional, for Resume Enhancement)
To demonstrate production MLOps (specifically around data drift and pipeline health):
* **Data Quality & Drift Monitoring:** Integrate **Evidently AI** or **Great Expectations** into the consumer groups. Set up automated alerts for when the simulated POS data schemas drift or when anomalous distributions are detected, proving you can catch silent failures before they hit downstream ML models.
