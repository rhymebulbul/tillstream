# Design Document 02: Data Governance, Serialization, & Schema Evolution

**Author:** Staff Data Engineer
**Status:** Approved
**Last Updated:** August 2026

## 1. Executive Summary
In a decoupled microservices architecture, schema drift is the primary cause of pipeline outages. TillStream implements a centralized Schema Registry pattern enforcing strict Apache Avro data contracts. This design prevents "poison pills" from entering the event stream and details our safe schema evolution strategies.

## 2. Serialization Format Trade-offs
*   **JSON:** Human-readable, but highly inefficient. Lacks strict typing, resulting in bloated payloads (keys repeated in every message). Rejected.
*   **Protobuf:** Excellent CPU efficiency and typing. However, Protobuf lacks a self-describing container format suitable for Data Lakes unless heavily modified.
*   **Decision (Apache Avro):** Selected for its compact binary format and deep integration with Hadoop/Spark ecosystems. Avro stores the schema alongside the data in batch files, making it the industry standard for Lakehouse architectures.

## 3. Schema Registry Architecture
TillStream utilizes Confluent Schema Registry as the central source of truth.

### 3.1 The Wire Format (Magic Byte Pattern)
To minimize payload bloat, the actual schema is *not* sent with every message. Instead, the producer sends a 5-byte header prepended to the binary payload:
1.  **Byte 0:** Magic Byte (Hardcoded to `0x00`). Identifies the payload as Confluent Avro.
2.  **Bytes 1-4:** 32-bit Integer representing the globally unique Schema ID.
3.  **Bytes 5+:** The raw Avro binary data.

### 3.2 High-Availability & Caching Strategy
If the Schema Registry goes down, the pipeline must not halt.
*   **Producer Cache:** Producers cache Schema IDs locally. A network call to the registry only occurs on application startup or when a new schema is registered.
*   **Consumer Cache:** Consumers maintain an LRU cache of `Schema ID -> Deserializer`. When a new ID is encountered, it fetches it from the registry and caches it permanently.

## 4. Schema Evolution Rules & Compatibility Modes
To prevent breaking changes, the Schema Registry enforces `BACKWARD` compatibility mode by default.

### 4.1 Backward Compatibility (Default)
Consumers using the *new* schema can read data produced by the *old* schema.
*   **Allowed:** Deleting a field (consumer ignores it), adding an optional field (with a `default` value).
*   **Rejected:** Adding a mandatory field without a default, renaming a field, changing data types (e.g., `string` to `int`).

### 4.2 Handling Conflict (HTTP 409)
If a developer's CI/CD pipeline attempts to register an incompatible schema, the Registry returns an `HTTP 409 Conflict`. The deployment fails, preventing the poison pill from ever reaching the production Kafka brokers.

## 5. Security & Access Control (ACLs)
*   **Subject-Level RBAC:** Only authorized CI/CD service accounts have `WRITE` access to the Schema Registry. Producers and Consumers operate with strictly `READ-ONLY` credentials to prevent runtime schema corruption.
