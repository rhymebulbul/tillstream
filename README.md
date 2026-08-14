<h1 align="center">
  🚀 TillStream
</h1>

<p align="center">
  <b>A FAANG-Grade Real-Time Data Platform with Autonomous Agentic Self-Healing</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Golang-1.22-00ADD8?style=for-the-badge&logo=go&logoColor=white" />
  <img src="https://img.shields.io/badge/Kafka_KRaft-3.7-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache_Iceberg-Lakehouse-0081C6?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Agentic_AI-Local_LLMs-FF6F00?style=for-the-badge" />
</p>

---

## 📖 Project Overview

**TillStream** is a massively scalable, multi-tenant Data Streaming Platform designed to process High-Throughput Point-of-Sale (POS) transactions. It combines traditional Data Engineering best practices (decoupled streams, strict schema governance, lakehouse analytics) with cutting-edge **Agentic GenAI** to autonomously heal data pipelines in real-time.

### 🌟 Key Highlights for Hiring Managers
- **Extreme Throughput:** Synthetic Golang generators capable of simulating 100k+ TPS across hundreds of multi-tenant retail stores.
- **Strict Data Contracts:** Confluent Schema Registry strictly enforces Avro payload structures, preventing upstream software bugs from polluting the Data Lake.
- **Modern Lakehouse Architecture:** PySpark Structured Streaming micro-batching into Apache Iceberg on MinIO/S3, querying via Trino (Presto).
- **Agentic AI Self-Healing:** An autonomous microservice powered by **Local LLMs (Qwen/Llama) & Gemini Pro** that intercepts schema violations in the Dead Letter Queue (DLQ), dynamically generates remediation Python code in a secure sandbox, and self-heals the pipeline without human intervention.
- **MLOps & Observability:** Integrated Prometheus latency tracking and data drift monitoring.

---

## 📸 Architecture & Action

### 1. High-Level Architecture
> *Place a screenshot of the system architecture diagram here.*
![Architecture Diagram](docs/assets/architecture.png)

### 2. The Agentic DLQ Resolver in Action
> *Place a screenshot of the terminal where the Agent generated the code, executed it, and healed the Kafka message here.*
![Agentic Healing](docs/assets/agent_healing.png)

---

## 🛠 Core Components

### 1. Ingestion Layer (Golang)
- `producer/`: A high-performance Golang application simulating massive concurrency using goroutines. It serializes data to binary Avro and publishes to Kafka.

### 2. Transport & Governance Layer (Kafka KRaft & Schema Registry)
- `infra/docker-compose.yml`: ZooKeeper-less Kafka (KRaft) cluster.
- **Schema Registry**: Centralized API storing Avro schemas for `com.tillstream.pos`. Ensures downstream consumers only receive structurally validated data.

### 3. Processing Layer (Python/PySpark)
- `consumers/main.py`: Consumes binary Avro, applies business logic (simulating database latency and timeouts), and routes anomalies to the DLQ.
- **Apache Iceberg**: PySpark micro-batches the validated Kafka streams into a scalable, ACID-compliant Lakehouse format.

### 4. Agentic Repair Layer (Autonomous AI)
- `agents/dlq_resolver.py`: The crown jewel of the platform. Instead of paging a Data Engineer at 2 AM, this Agent reads corrupted hex payloads from the DLQ, prompts an LLM (local Qwen Coder or cloud Gemini), dynamically generates `fix_payload(raw_bytes)` code, executes the mutation in a sandbox, and replays the corrected data back into the pipeline.

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- Optional: [Ollama](https://ollama.com/) for Local LLM Agentic execution.

### Spin up the Infrastructure
```bash
cd infra
docker-compose up -d
```
*(Brings up Kafka KRaft broker on port 29092, Schema Registry on 8081)*

### Run the Agentic Self-Healing Demo
See [`agents/README.md`](agents/README.md) for full, copy-pasteable instructions on how to trigger a pipeline crash and watch the Local LLM automatically heal it in real-time.

---

## 📚 Technical Design Documents

For deep-dives into the engineering decisions, trade-offs, and FAANG-level system designs, see the `docs/design` directory:

1. [High-Level Architecture](docs/design/01_high_level_architecture.md)
2. [Data Governance & Schema Evolution](docs/design/02_data_governance_schema.md)
3. [Producer/Consumer Patterns & Scaling](docs/design/03_producer_consumer_patterns.md)
4. [Lakehouse & Analytics](docs/design/04_lakehouse_analytics.md)
5. [Observability & MLOps](docs/design/05_observability_mlops.md)

---

<p align="center">
  <i>Built to demonstrate the bridge between high-throughput Data Engineering and autonomous AI.</i>
</p>
