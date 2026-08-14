# Phase 0 Jiras (Environment & Foundations)

**Goal:** Establish the local development infrastructure required for the TillStream project.

## TILL-01: Create Docker Compose Infrastructure Skeleton
* **Description:** Create the foundational `docker-compose.yml` to run Apache Kafka in KRaft mode (no Zookeeper).
* **Acceptance Criteria:**
  * `docker-compose up -d` successfully starts a single-node Kafka broker.
  * Kafka broker is reachable on `localhost:9092`.
  * No Zookeeper container is used.

## TILL-02: Add Schema Registry to Infrastructure
* **Description:** Add Confluent Schema Registry to the docker-compose stack to manage Avro schemas.
* **Acceptance Criteria:**
  * Schema registry container starts and connects to the Kafka broker.
  * Schema registry API is accessible on `localhost:8081`.

## TILL-03: Add Observability Stack (Prometheus & Grafana)
* **Description:** Integrate monitoring tools to track Kafka metrics, which will be essential for Phase 5 (Chaos) and Phase 6 (Observability).
* **Acceptance Criteria:**
  * Add Prometheus container configured to scrape Kafka metrics.
  * Add Grafana container accessible on `localhost:3000`.
  * Add Kafka Exporter (or JMX Exporter) to expose broker metrics to Prometheus.
