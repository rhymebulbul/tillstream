# Design Document 04: The Lakehouse Architecture (Iceberg + Trino)

**Author:** Staff Data Engineer
**Status:** Approved
**Last Updated:** August 2026

## 1. Executive Summary
Traditional Data Warehouses (Snowflake, BigQuery) are expensive for petabyte-scale storage, while traditional Data Lakes (raw S3 buckets) suffer from "Data Swamp" symptoms (lack of ACID transactions, corrupted schemas). TillStream implements an open-source "Lakehouse" architecture, combining the cost-efficiency of S3 object storage with the transactional guarantees of a relational database.

## 2. Storage & Table Format

### 2.1 MinIO (Object Storage)
*   Provides a highly available, S3-compatible API.
*   **Consistency Model:** Modern S3/MinIO provides strict read-after-write consistency, which is absolutely mandatory for Iceberg's optimistic concurrency control to function correctly during concurrent writes.

### 2.2 Apache Iceberg (Table Format)
Dumping Parquet files directly into S3 results in extremely slow queries because the query engine must perform an `S3 LIST` across millions of objects to find relevant data.
*   **The Metadata Tree:** Iceberg completely eliminates `S3 LIST` operations. It maintains a hierarchical metadata tree: `Metadata.json -> Manifest List -> Manifest File -> Data File (Parquet)`.
*   **Atomic Commits:** When PySpark writes a micro-batch, it generates new Parquet files and a new `Metadata.json`. The pointer to the current snapshot is swapped atomically. If the Spark job fails mid-write, the corrupted files are simply never referenced by the metadata tree (Zero dirty reads).
*   **Time Travel & Rollbacks:** Because old snapshots are preserved, analysts can query the lake *as it existed* at any specific timestamp, or roll back the entire table if a bad batch is ingested.

## 3. Ingestion Engine (PySpark Structured Streaming)
*   **Trigger Interval:** `ProcessingTime='60 seconds'`. Real-time Kafka events are micro-batched to prevent the "Small Files Problem" on S3.
*   **Checkpointing:** Spark writes offsets to a Write-Ahead Log (WAL) checkpoint directory in MinIO. If the Spark master node crashes, the replacement node reads the checkpoint and resumes exactly where it left off.

## 4. Analytical Query Engine (Trino)
Trino is a Massively Parallel Processing (MPP) SQL query engine.
*   **Architecture:** Trino consists of a single Coordinator and fleet of Workers.
*   **Query Execution:** When an analyst executes `SELECT SUM(price) FROM iceberg.lakehouse.orders WHERE date = 'today'`, the Trino Coordinator reads the Iceberg Metadata tree. 
*   **Predicate Pushdown:** The Coordinator extracts column statistics (Min/Max values) stored inside the Iceberg Manifests. It identifies exactly which underlying Parquet files contain data for 'today', skipping 99% of the S3 files completely (O(1) file pruning).
*   **Worker Distribution:** The Coordinator assigns the targeted Parquet files to the Worker fleet, which streams the data into memory, performs the aggregation, and returns the scalar result in milliseconds.
