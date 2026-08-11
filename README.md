# TillStream 🏪⚡

![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)

TillStream is a high-throughput, horizontally scalable Data Platform designed to ingest, serialize, and process synthetic Point-of-Sale (POS) transactions. It simulates a multi-tenant retail environment where a small percentage of "Flagship" tenants generate the vast majority of the transaction volume.

## 🏗️ Architecture
1. **Go Event Generator (Producers)**: A containerized Go application that generates realistic mock POS `orders` and `payments` using `gofakeit`. It implements an 80/20 volume skew algorithm to simulate real-world tenant hotspots.
2. **Confluent Schema Registry**: Enforces strict schema evolution policies and serializes the JSON data into highly efficient Avro binaries before they hit the wire.
3. **Apache Kafka (KRaft Mode)**: The core event broker, running without ZooKeeper, partitioning data heavily by `tenant_id` across multiple brokers.
4. **Observability Stack**: Prometheus and Grafana (via Kafka Exporter) to visualize partition skew, message-in rates, and consumer lag.

## 🚀 Key Engineering Highlights
* **Schema Evolution**: Strict Avro serialization. The platform actively rejects breaking schema changes (HTTP 409) to protect downstream consumers from data corruption.
* **Hot Partition Management**: Designed to demonstrate the challenge of tenant-based partitioning when high-volume tenants monopolize specific partitions, and how to alleviate it via partition scaling.
* **Developer Standards**: Strictly adheres to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) and atomic, feature-branch-driven Jira ticket lifecycles.

## 🛠️ Quick Start

Ensure you have Docker and Docker Compose installed on your machine.

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/tillstream.git
cd tillstream

# 2. Spin up the entire data platform (Kafka, Schema Registry, Grafana, Prometheus, Go Producer)
docker compose up -d --build

# 3. View the live telemetry
# Open http://localhost:3000 in your browser (admin/admin)
# Navigate to the "Kafka Dashboards" folder and open the "Kafka Exporter Overview"
```

## 📂 Project Structure
* `/producers/` - The Go application that generates and Avro-serializes POS events.
* `/infra/` - Initialization scripts and provisioning configurations for Grafana/Prometheus.
* `/docker-compose.yml` - The infrastructure definition for the entire stack.
