# Design Doc 04: The Lakehouse Architecture

## Motivation
While Kafka is exceptional at real-time, low-latency streaming, it is fundamentally an append-only log. It is not designed for complex `JOIN`s, historical aggregations, or Data Science workloads. 

To bridge this gap, TillStream implements a **Streaming Lakehouse**, decoupling cheap storage from massive compute.

## Architecture & Implementation

### 1. The Storage Layer (MinIO)
*   **S3 Compatibility:** MinIO is deployed as a local, high-performance object storage layer. It provides an identical API to AWS S3, meaning code written for TillStream can be deployed directly to AWS without modification.
*   **Cost Efficiency:** Object storage is infinitely scalable and magnitudes cheaper than traditional relational databases.

### 2. The Table Format (Apache Iceberg)
Dumping raw JSON/Avro files into S3 creates a "Data Swamp" (slow queries, no ACID transactions, schema chaos).
*   **ACID Transactions:** TillStream uses **Apache Iceberg** as its open table format. Iceberg tracks metadata for every file, allowing for atomic commits, time-travel queries, and schema evolution directly on data lake storage.

### 3. The Ingestion Engine (PySpark Structured Streaming)
*   **Micro-Batching:** A PySpark cluster subscribes to the Kafka `orders` topic. 
*   **Direct-to-Lake:** Every 60 seconds, Spark flushes the newly arrived Kafka events directly into Iceberg tables in MinIO, formatting the underlying files as highly compressed, columnar Parquet.

### 4. The Query Engine (Trino)
*   **Massive Parallel Processing (MPP):** Trino (formerly PrestoSQL) is deployed to sit on top of the Lakehouse.
*   **Analyst Access:** Data Analysts and BI tools (like Tableau) connect to Trino via standard SQL. Trino distributes the query, reads the Iceberg metadata, and rapidly scans the Parquet files in MinIO to return aggregations in milliseconds.
