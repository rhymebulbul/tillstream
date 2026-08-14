# Phase 7 Jiras (The Lakehouse Extension)

**Goal:** Expand the platform from a pure event streaming architecture into a modern Lakehouse architecture, demonstrating batch/streaming convergence.

## TILL-18: Deploy MinIO and Spark Structured Streaming
* **Description:** Spin up a local S3-compatible object store (MinIO) and write a Spark Structured Streaming job to ingest the Kafka topics into Apache Iceberg tables.
* **Acceptance Criteria:**
  * Add MinIO to `docker-compose.yml`.
  * Write a PySpark job that consumes from `orders` and writes to an Iceberg catalog in MinIO.
  * Handle Avro schema inference automatically within Spark.

## TILL-19: Implement Trino Query Layer
* **Description:** Deploy Trino to provide a high-performance, distributed SQL query engine over the Iceberg tables in MinIO.
* **Acceptance Criteria:**
  * Add Trino to `docker-compose.yml`.
  * Configure Trino to connect to the Iceberg catalog.
  * Execute standard SQL queries (e.g., aggregations by tenant) against the raw streaming data.
