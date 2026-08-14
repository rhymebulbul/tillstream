# TillStream: Lakehouse Architecture (Iceberg + Spark + Trino)

| Field | Value |
|---|---|
| **Document ID** | TILL-DESIGN-004 |
| **Status** | Approved |
| **Author** | Staff Data Engineer |
| **Last Updated** | August 2026 |
| **Reviewers** | Principal Engineer (Platform), Data Science Lead, Staff SRE |

---

## 1. Executive Summary

TillStream's Lakehouse layer solves a fundamental tension in modern data platforms: **real-time event data must be simultaneously queryable for operational analytics, ML feature engineering, and ad-hoc exploration — without the cost of a managed data warehouse or the unreliability of a raw object store.**

The architecture combines:

- **Apache Kafka** as the durable, replayable event log (source of truth)
- **PySpark Structured Streaming** as the ingestion engine, micro-batching Kafka events into Parquet files
- **Apache Iceberg** as the transactional table format, providing ACID guarantees over S3-compatible object storage
- **MinIO** as the S3-compatible object store (production: AWS S3 or GCS)
- **Trino** as the MPP query engine, providing sub-second SQL analytics over petabyte-scale Iceberg tables

This design avoids the "Data Swamp" problem (raw S3 buckets without ACID transactions or schema enforcement) and the prohibitive cost of managed warehouses (Snowflake/BigQuery at petabyte scale).

---

## 2. The Data Swamp Problem: Why Raw S3 Is Insufficient

Writing Parquet files directly to S3 without a table format layer creates the following failure modes:

| Problem | Symptom | Cause |
|---|---|---|
| **Slow query planning** | Analyst queries take minutes to return even for simple `COUNT(*)` | Query engine must issue `S3 LIST` across millions of objects to find relevant files |
| **Dirty reads** | Queries return partial results mid-write | No ACID isolation; concurrent writers create partially-visible file sets |
| **Schema corruption** | New columns cause downstream reads to fail | No schema enforcement; files with different schemas co-exist in the same "table" directory |
| **No time travel** | Accidental data overwrites are permanent | No snapshot management; once a file is deleted, its data is gone |
| **Small files problem** | Metadata overhead dominates I/O time | Streaming workloads produce thousands of tiny files per hour |

Apache Iceberg addresses all five failure modes simultaneously.

---

## 3. Apache Iceberg Table Format

### 3.1 Metadata Tree Architecture

Iceberg maintains a hierarchical metadata tree that decouples the logical table view from the physical file layout on object storage:

```
s3a://lakehouse/warehouse/raw/orders/
│
├── metadata/
│   ├── v1.metadata.json              ← Snapshot 1 (historical)
│   ├── v2.metadata.json              ← Snapshot 2 (current pointer)
│   ├── snap-001-manifest-list.avro   ← Manifest List for Snapshot 1
│   ├── snap-002-manifest-list.avro   ← Manifest List for Snapshot 2
│   ├── manifest-001.avro             ← Manifest: lists data files + column stats
│   └── manifest-002.avro             ← Manifest: lists data files + column stats
│
└── data/
    ├── date=2026-08-14/tenant_id=TENANT_FLAGSHIP_1/
    │   ├── 00001.parquet
    │   └── 00002.parquet
    └── date=2026-08-14/tenant_id=TENANT_LOCAL_1/
        └── 00003.parquet
```

**Query planning is O(1):** The Trino coordinator reads `v2.metadata.json` → follows the pointer to `snap-002-manifest-list.avro` → reads only the manifests for the relevant partition → extracts column-level Min/Max statistics to prune irrelevant Parquet files. **Zero `S3 LIST` operations are issued.**

### 3.2 ACID Guarantees via Optimistic Concurrency Control

Iceberg uses an **optimistic concurrency control (OCC)** model for concurrent writers:

1. Writer reads the current `metadata.json` pointer (snapshot N).
2. Writer generates new Parquet files and a new `metadata.json` (snapshot N+1).
3. Writer attempts to atomically swap the `current-snapshot-id` pointer in the catalog (using either a database-backed catalog or S3 conditional write semantics).
4. If another writer committed snapshot N+1 in the meantime, the current writer's commit fails with a `CommitFailedException`. The writer retries from step 1.

**MinIO consistency requirement:** This OCC model requires **strong read-after-write consistency** from the object store. MinIO provides this natively (AWS S3 also provides strong consistency since 2020). Eventual consistency object stores (e.g., early S3) are incompatible with Iceberg's OCC model.

### 3.3 Atomic Failure Recovery

If the PySpark job crashes mid-write (after generating Parquet files but before committing the metadata pointer):

- The orphan Parquet files are written to S3 but are never referenced by any Iceberg snapshot.
- No query engine will ever read these files — they are invisible to the metadata tree.
- Iceberg's `ExpireSnapshots` and `DeleteOrphanFiles` procedures (scheduled via Airflow) will garbage-collect them.

**Zero dirty reads.** This is the critical correctness guarantee that raw S3 cannot provide.

### 3.4 Time Travel & Audit Queries

Because all historical snapshots are preserved until explicitly expired, Trino can query the Lakehouse as it existed at any point in time:

```sql
-- Query the state of the orders table 7 days ago
SELECT COUNT(*), SUM(total_price)
FROM lakehouse.raw.orders FOR SYSTEM_TIME AS OF TIMESTAMP '2026-08-07 00:00:00'
WHERE tenant_id = 'TENANT_FLAGSHIP_1';

-- Roll back the entire table to a specific snapshot after a bad ingestion
CALL lakehouse.system.rollback_to_snapshot('raw.orders', 123456789);
```

This is essential for ML model auditing (what data was the model trained on at time T?) and for recovering from accidental bad batch ingestion without requiring a full pipeline replay.

---

## 4. PySpark Structured Streaming Ingestion

### 4.1 Job Configuration

From [`lakehouse/spark/stream_to_iceberg.py`](../../lakehouse/spark/stream_to_iceberg.py):

```python
spark = SparkSession.builder \
    .appName("TillStream_Lakehouse_Ingestion") \
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
        "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hadoop") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
```

**Catalog type note:** `type=hadoop` uses the filesystem as the Iceberg catalog. This is appropriate for development but has a known limitation: concurrent multi-writer commits are not serialized through a central catalog, relying purely on S3 conditional puts. Production deployments at scale should use a **Hive Metastore (HMS)** or **AWS Glue** catalog to serialize concurrent commits through a database transaction, eliminating commit races.

### 4.2 Micro-Batch Trigger Strategy

```python
query = kafka_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \    # Key parameter
    .option("path", "lakehouse.raw.orders") \
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/orders") \
    .start()
```

**Why 60 seconds, not continuous?** PySpark's `Trigger.Continuous` mode (true stream processing) writes one file per micro-second of events — producing millions of tiny Parquet files per day. Each file has a fixed overhead of an Iceberg manifest entry and an S3 metadata record. Query planning time grows linearly with the number of files. The 60-second trigger batches ~6M events per file at target throughput, producing ~1,440 files/day — a manageable metadata footprint.

**The Small Files Problem:** If the trigger interval is too short (< 10 seconds at high throughput), query planning overhead dominates query execution time. Iceberg's `rewrite_data_files` compaction procedure (scheduled via Airflow, target file size 128 MB–1 GB) consolidates small files periodically without interrupting concurrent reads.

### 4.3 Exactly-Once Delivery to Iceberg

The Spark Structured Streaming job achieves exactly-once semantics through two mechanisms:

1. **Kafka offset checkpointing:** Spark writes its Kafka consumer offsets to `s3a://lakehouse/checkpoints/orders` (a Write-Ahead Log). If the Spark driver crashes, the replacement driver reads the checkpoint and restarts exactly from the last committed Kafka offset.

2. **Iceberg commit atomicity:** The Parquet files for each micro-batch are written first, then the Iceberg snapshot is atomically committed. If the job crashes between file write and commit, the orphan files are never visible (as described in §3.3). On restart, Spark re-processes the batch from the WAL offset, re-generates the same files, and completes the commit. Iceberg's deduplication logic (based on sequence numbers) ensures the second commit is a no-op for already-committed data.

### 4.4 Schema Integration: Confluent Wire Format in Spark

The current `stream_to_iceberg.py` reads raw Kafka byte values without Avro deserialization:

```python
# Note: In a true production environment with Confluent Schema Registry,
# you would use the ABRiS library (za.co.absa:abris) to dynamically pull schemas
# and strip the 5-byte Confluent Magic Byte.
```

**Production requirement:** The 5-byte Confluent wire header (magic byte + schema ID) must be stripped before writing to Iceberg. The [ABRiS library](https://github.com/AbsaOSS/ABRiS) (`za.co.absa:abris`) provides native Spark schema evolution integration with the Confluent Schema Registry. It handles:

1. Dynamic schema fetching from the Schema Registry via the schema ID in the wire header
2. Avro binary deserialization within Spark's distributed executor context
3. Automatic schema evolution when new schema versions are registered

Until ABRiS is integrated, the Lakehouse table contains raw binary columns rather than typed columns, making it unqueryable via Trino.

---

## 5. Trino MPP Query Engine

### 5.1 Architecture

Trino follows a coordinator/worker architecture:

```
┌──────────────────────────────────────────┐
│            Trino Coordinator             │
│                                          │
│  1. Parse SQL                            │
│  2. Read Iceberg metadata tree           │
│  3. Apply predicate pushdown             │
│     (column-level Min/Max statistics)    │
│  4. Identify relevant Parquet files      │
│  5. Split files into tasks               │
│  6. Schedule tasks on Worker fleet       │
│  7. Aggregate partial results            │
│  8. Return final result to client        │
└──────────────────┬───────────────────────┘
                   │  Task distribution
        ┌──────────┼──────────┐
        ▼          ▼          ▼
  ┌──────────┐ ┌──────────┐ ┌──────────┐
  │  Worker  │ │  Worker  │ │  Worker  │
  │          │ │          │ │          │
  │  Read    │ │  Read    │ │  Read    │
  │  Parquet │ │  Parquet │ │  Parquet │
  │  files   │ │  files   │ │  files   │
  │  Stream  │ │  Stream  │ │  Stream  │
  │  to mem  │ │  to mem  │ │  to mem  │
  └──────────┘ └──────────┘ └──────────┘
```

### 5.2 Predicate Pushdown & File Pruning

When an analyst executes a query like:

```sql
SELECT tenant_id, SUM(total_price) AS revenue
FROM lakehouse.raw.orders
WHERE date = '2026-08-14'
  AND tenant_id = 'TENANT_FLAGSHIP_1'
GROUP BY tenant_id;
```

Trino's query planner executes the following steps before any data is transferred from MinIO:

1. **Metadata scan:** Read `v2.metadata.json` → identify partition columns (`date`, `tenant_id`)
2. **Partition pruning:** Evaluate `date = '2026-08-14'` against the partition spec → skip all other date directories
3. **Manifest read:** Read only the manifests for `date=2026-08-14/tenant_id=TENANT_FLAGSHIP_1/`
4. **Column statistics check:** Each manifest entry contains Min/Max values for every column. Apply additional column predicates to skip Parquet files where the predicate cannot match.
5. **Result:** Only the 2 Parquet files in `date=2026-08-14/tenant_id=TENANT_FLAGSHIP_1/` are fetched from MinIO and distributed to workers.

At 18 TB/day of raw data (see TILL-DESIGN-001 §6.2), a single-day, single-tenant query scans < 0.1% of total data volume. This is the O(1) pruning property of Iceberg's metadata tree in action.

### 5.3 Catalog Configuration

From [`infra/trino/catalog/`](../../infra/trino/catalog/):

```properties
# lakehouse.properties
connector.name=iceberg
iceberg.catalog.type=hadoop
hive.metastore.uri=...  # or filesystem path for hadoop catalog
```

In the current Docker Compose environment, Trino connects to the Iceberg tables directly via the `hadoop` catalog (filesystem-backed). For production, the catalog should be backed by a Hive Metastore or AWS Glue to enable concurrent multi-writer commit serialization.

---

## 6. Table Design & Partitioning Strategy

### 6.1 Current Partition Spec

The `lakehouse.raw.orders` table is partitioned by:

```sql
-- Iceberg DDL (target production schema)
CREATE TABLE lakehouse.raw.orders (
    order_id       VARCHAR,
    tenant_id      VARCHAR,
    store_id       VARCHAR,
    customer_id    VARCHAR,
    loyalty_points INTEGER,
    total_price    DOUBLE,
    created_at     TIMESTAMP WITH TIME ZONE
)
USING iceberg
PARTITIONED BY (date(created_at), tenant_id)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes' = '134217728'  -- 128 MB target file size
);
```

**Rationale for `date(created_at)` + `tenant_id` partitioning:**
- Most analytical queries are scoped to a date range — this is the highest-cardinality filter eliminating the most data.
- Secondary partitioning by `tenant_id` satisfies the majority of tenant-scoped operational queries without a full date-partition scan.
- The combination creates a partition directory per `(date, tenant)` — at 5 tenants and 365 days, this is 1,825 partition directories, well within Iceberg's efficient metadata handling range.

### 6.2 Compaction (Maintenance Operations)

The 60-second micro-batch trigger produces ~1,440 micro-batch commits per day. Each commit adds at least one new Parquet file. Without compaction, query planning time grows as manifests accumulate.

Scheduled via Airflow (Phase 7):

```sql
-- Compact small files into 128 MB target files (run daily)
CALL lakehouse.system.rewrite_data_files(
    table => 'raw.orders',
    options => map(
        'target-file-size-bytes', '134217728',
        'min-input-files', '10'
    )
);

-- Expire snapshots older than 7 days (align with Kafka retention)
CALL lakehouse.system.expire_snapshots(
    table => 'raw.orders',
    older_than => TIMESTAMP '2026-08-07 00:00:00',
    retain_last => 10
);

-- Remove orphan files (from crashed Spark jobs)
CALL lakehouse.system.delete_orphan_files(
    table => 'raw.orders',
    older_than => TIMESTAMP '2026-08-13 00:00:00'
);
```

---

## 7. Known Limitations & Future Work

| Item | Description | Priority |
|---|---|---|
| ABRiS integration | Avro deserialization in Spark is not implemented; Lakehouse writes raw bytes | **P0 — Lakehouse is not queryable without this** |
| Hadoop catalog | No concurrent-writer serialization; production requires HMS or Glue | High |
| `created_at` as string | Field is currently `VARCHAR`, not `TIMESTAMP`; Trino time-based queries require casting | Medium |
| Compaction scheduling | No Airflow DAG defined; metadata footprint grows unbounded | High |
| Credential management | MinIO credentials hardcoded in `stream_to_iceberg.py`; must be moved to Vault/SSM | P0 for production |
| `total_price` as `DOUBLE` | Financial precision lost; `DECIMAL(12,2)` required | High |

---

*Related Documents: [TILL-DESIGN-001 High-Level Architecture](./01-high-level-architecture.md) | [TILL-DESIGN-003 Producer/Consumer Patterns](./03-producer-consumer-patterns.md) | [TILL-DESIGN-005 Observability & MLOps](./05-observability-and-mlops.md)*
