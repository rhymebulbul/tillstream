# TillStream: Data Governance, Serialization, & Schema Evolution

| Field | Value |
|---|---|
| **Document ID** | TILL-DESIGN-002 |
| **Status** | Approved |
| **Author** | Staff Data Engineer |
| **Last Updated** | August 2026 |
| **Reviewers** | Principal Engineer (Platform), Staff SRE |

---

## 1. Executive Summary

In any decoupled, microservices-oriented data platform, **schema drift is the primary cause of silent pipeline outages.** An incompatible schema change — a renamed field, a deleted mandatory column, a type change from `string` to `int` — will cause consumers to throw deserialization exceptions. If those exceptions are unhandled, they halt the consumer entirely (head-of-line blocking). If they are silently swallowed, they corrupt the Lakehouse with empty or null-padded records.

TillStream enforces a centralized schema contract layer via **Confluent Schema Registry** and the **Apache Avro binary format**, mandating that every byte written to a Kafka topic is validated against a registered schema before transmission. Schema changes that violate backward compatibility are rejected at the CI/CD gate — they never reach a production broker.

This document covers: serialization format selection, the Confluent wire protocol, Schema Registry high-availability design, compatibility mode semantics, CI/CD enforcement, and access control.

---

## 2. Serialization Format Selection

### 2.1 Requirements

The serialization format must satisfy:
- **Type safety:** Prevent ambiguous type coercion (e.g., a numeric price silently becoming a string).
- **Payload efficiency:** Binary encoding; keys must not be repeated per message.
- **Lakehouse compatibility:** The format must be readable by PySpark without a separate schema lookup at scan time.
- **Schema evolution:** Must support additive changes without requiring synchronized producer/consumer deployments.

### 2.2 Comparison Matrix

| Property | JSON | Protocol Buffers | Apache Avro |
|---|---|---|---|
| **Encoding** | Text (UTF-8) | Binary | Binary |
| **Payload efficiency** | ❌ Keys repeated in every message; ~3-5x larger than binary | ✅ Compact varint encoding | ✅ Schema-defined; no field names in payload |
| **Type safety** | ❌ No schema enforcement; `"123"` and `123` are both valid | ✅ Strict `.proto` type definitions | ✅ Strict Avro schema type definitions |
| **Schema evolution** | ❌ No native contract enforcement | ✅ `optional` fields are backward compatible | ✅ Native `BACKWARD`/`FORWARD`/`FULL` compatibility modes |
| **Lakehouse compatibility** | ⚠️ Spark can read JSON; no schema embedding | ⚠️ Requires `.proto` files distributed to all readers | ✅ Schema embedded in `.avro` batch files; Spark reads natively |
| **Schema Registry support** | ❌ No native integration | ⚠️ Supported, but less common | ✅ First-class Confluent SR integration |

**Decision: Apache Avro** is selected because it uniquely satisfies all four requirements simultaneously. Its self-describing batch format (schema embedded in `.avro` files) is particularly critical for Lakehouse workloads — a PySpark job reading Iceberg-stored Parquet files converted from Avro does not need a live Schema Registry connection to reconstruct field types.

---

## 3. Event Schemas

### 3.1 `orders` Topic Schema (`com.tillstream.pos.Order`)

Defined in [`producers/cmd/producer/main.go`](../../producers/cmd/producer/main.go), registered against subject `orders-value`:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.tillstream.pos",
  "fields": [
    {"name": "order_id",       "type": "string"},
    {"name": "tenant_id",      "type": "string"},
    {"name": "store_id",       "type": "string"},
    {"name": "customer_id",    "type": "string"},
    {"name": "loyalty_points", "type": "int",    "default": 0},
    {"name": "total_price",    "type": "double"},
    {"name": "created_at",     "type": "string"}
  ]
}
```

**Design notes:**
- `loyalty_points` carries `"default": 0` — this is intentional. Any new consumer that reads historical data produced before this field existed will receive `0` rather than a deserialization error.
- `created_at` is an ISO 8601 RFC3339 string (`time.Now().UTC().Format(time.RFC3339)`) rather than Avro's `{"type": "long", "logicalType": "timestamp-millis"}`. This is a known limitation: the logical type is preferred for Parquet column encoding efficiency and Trino `TIMESTAMP` type inference. Filed as tech debt for schema v2.
- `total_price` is a `double` (IEEE 754 64-bit float). For financial applications, `{"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}` is the correct representation. This is a Phase 1 simplification; migration path is additive (add new `total_price_decimal` field as optional with default).

### 3.2 `payments` Topic Schema (`com.tillstream.pos.Payment`)

Registered against subject `payments-value`:

```json
{
  "type": "record",
  "name": "Payment",
  "namespace": "com.tillstream.pos",
  "fields": [
    {"name": "payment_id",     "type": "string"},
    {"name": "order_id",       "type": "string"},
    {"name": "tenant_id",      "type": "string"},
    {"name": "amount",         "type": "double"},
    {"name": "payment_method", "type": "string"},
    {"name": "status",         "type": "string"},
    {"name": "created_at",     "type": "string"}
  ]
}
```

**Design note:** `payment_method` and `status` are currently `string` types. In production, these should be `{"type": "enum", "name": "PaymentMethod", "symbols": ["CREDIT_CARD", "DEBIT_CARD", "CASH", "DIGITAL_WALLET"]}`. Enum types enable Avro to encode the value as an integer index rather than a variable-length string, and provide compile-time validation in statically typed producers. Migration path requires a `FULL_TRANSITIVE` compatibility check.

---

## 4. Confluent Wire Protocol

### 4.1 Wire Format Specification

TillStream uses the **Confluent Avro wire format** to eliminate per-message schema repetition. The actual Avro schema is registered once in the Schema Registry and referenced by ID in every subsequent message. The wire format for every Kafka message value is:

```
┌─────────────────────────────────────────────────────────┐
│  Byte 0     │  Bytes 1-4        │  Bytes 5+             │
│  Magic Byte │  Schema ID        │  Avro Binary Payload  │
│  0x00       │  uint32 big-endian│  (schemaless)         │
└─────────────────────────────────────────────────────────┘
```

- **Magic Byte (`0x00`):** Hardcoded sentinel identifying the payload as a Confluent Avro message. A consumer receiving any other magic byte must treat the message as a poison pill and route it to the DLQ immediately, without attempting deserialization.
- **Schema ID (4 bytes, big-endian uint32):** A globally unique integer assigned by the Schema Registry when a schema is first registered. This ID is stable for the lifetime of the schema version.
- **Avro Payload (schemaless):** Raw Avro binary encoding of the record, written without the full schema — just the field values in schema-defined order.

### 4.2 Producer Implementation

From [`producers/internal/kafka/producer.go`](../../producers/internal/kafka/producer.go):

```go
// EncodeAvroWithMagicByte prepends the 5-byte Confluent wire-format header
func EncodeAvroWithMagicByte(schemaID int, avroBytes []byte) []byte {
    header := make([]byte, 5)
    header[0] = 0  // Magic byte
    binary.BigEndian.PutUint32(header[1:], uint32(schemaID))
    return append(header, avroBytes...)
}
```

The `schemaID` is obtained once at producer startup via `RegisterSchema()` and reused for all subsequent messages. This means Schema Registry is **not** on the critical ingestion path during steady-state operation.

### 4.3 Consumer Implementation

From [`consumers/main.py`](../../consumers/main.py):

```python
# Decode wire header
magic, schema_id = struct.unpack('>bI', payload[:5])

if magic != 0:
    # Route to DLQ immediately — unknown wire format
    dlq_producer.produce('orders-dlq', value=payload, key=msg.key())
    continue

# LRU schema cache: fetch from SR only on cache miss
if schema_id not in schema_cache:
    schema = sr_client.get_schema(schema_id)
    schema_cache[schema_id] = fastavro.parse_schema(json.loads(schema.schema_str))

record = fastavro.schemaless_reader(BytesIO(payload[5:]), schema_cache[schema_id])
```

The `schema_cache` dictionary grows monotonically as new schema IDs are encountered. In practice, the number of distinct schema IDs per topic is very small (one per schema version), making the cache effectively permanent for the lifetime of the consumer process.

---

## 5. Schema Registry Architecture

### 5.1 Topology

```
┌─────────────────────────────────────────────┐
│         Confluent Schema Registry            │
│         confluentinc/cp-schema-registry:7.6.0│
│         http://schema-registry:8081          │
│                                              │
│  Backing store: Kafka internal topic         │
│  (_schemas, replication.factor=3)            │
│                                              │
│  Subjects:                                   │
│  ├── orders-value   (BACKWARD compatibility) │
│  └── payments-value (BACKWARD compatibility) │
└─────────────────────────────────────────────┘
        ▲                         ▲
        │                         │
  [Producer]                [Consumer]
  RegisterSchema()          get_schema(id)
  (startup only)            (cache miss only)
```

### 5.2 High-Availability & Failure Modes

The Schema Registry's role differs between producers and consumers:

| Component | SR Dependency | Behavior on SR Outage |
|---|---|---|
| **Go Producer** | Calls `RegisterSchema` once on startup | **Blocked at startup;** if SR recovers, producer starts normally. In-flight messages are unaffected (schema ID cached). |
| **Python Consumer** | Fetches schema on first encounter of a new schema ID | **Transparent for known IDs** (served from `schema_cache`). A brand-new schema ID with SR offline causes a fetch failure → message routed to DLQ. |
| **PySpark Streaming** | Reads schema at job initialization | **Job startup blocked;** running jobs unaffected (schema in executor memory). |

**Implication:** The Schema Registry is a **startup-time dependency**, not a runtime dependency for steady-state message processing. This is the key high-availability property of the Confluent wire format design.

---

## 6. Schema Evolution & Compatibility Modes

### 6.1 Compatibility Mode Semantics

The Schema Registry enforces one of four compatibility policies per subject. TillStream uses `BACKWARD` as the default for all production subjects.

| Mode | Definition | Allowed Operations | Rejected Operations |
|---|---|---|---|
| `BACKWARD` | New schema consumers can read data written by the previous schema | Delete a field; add an optional field with `default` | Add a required field (no default); rename a field; change type |
| `FORWARD` | Old schema consumers can read data written by the new schema | Add a new field; delete an optional field with `default` | Delete a required field |
| `FULL` | Both backward AND forward compatible | Only add/delete fields with defaults | Any breaking change |
| `NONE` | No compatibility checking | Any change allowed | Nothing rejected |

**TillStream policy:** All production subjects use `BACKWARD`. This allows a rolling deployment where new consumers are deployed before new producers — the new consumer code handles both the old and new schema versions simultaneously. Producers can then be upgraded without coordinating a consumer restart.

### 6.2 Safe Evolution Patterns

#### ✅ Adding an optional field (allowed)

```json
// Schema v1 → v2: Adding optional discount_code field
{"name": "discount_code", "type": ["null", "string"], "default": null}
```

- Consumers using schema v2 reading v1 messages will receive `null` for `discount_code`.
- No consumer restart required.

#### ✅ Removing a field with a default (allowed under BACKWARD)

A field removed from the producer schema will be defaulted for any consumer still referencing the old schema. However, consumers must be updated before the field is removed from the schema.

#### ❌ Renaming a field (rejected)

Avro does not have rename semantics. A rename is treated as deleting the old field and adding a new required field — both operations that break backward compatibility. The correct approach is:

1. Register schema v2 with both old and new field names (as aliases): `"aliases": ["old_field_name"]`
2. Deploy consumers that read either field name
3. Register schema v3 dropping the old field name

#### ❌ Changing `total_price` from `double` to `decimal` (breaking)

This requires a `FULL_TRANSITIVE` compatibility window or a new topic (`orders-v2`) with a migration consumer that reads from `orders` and republishes to `orders-v2` with the converted type.

### 6.3 CI/CD Enforcement

Schema changes must be validated in CI before merging. The Schema Registry REST API provides a compatibility check endpoint:

```bash
# CI/CD validation step (pre-merge)
curl -X POST \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<escaped-new-schema-json>"}' \
  http://schema-registry:8081/compatibility/subjects/orders-value/versions/latest

# Expected: {"is_compatible": true}
# On incompatible change: HTTP 200, {"is_compatible": false} → CI step fails
# On registration attempt with incompatible schema: HTTP 409 Conflict → deployment blocked
```

A `409 Conflict` from the Schema Registry during deployment means the new schema is incompatible with the registered version. **The deployment pipeline must treat this as a hard failure** — the producer binary is not deployed, and no incompatible message ever reaches a production Kafka partition.

---

## 7. Access Control (ACLs)

| Principal | Access Level | Rationale |
|---|---|---|
| `ci-cd-service-account` | `WRITE` (subject-level) | Only CI/CD pipelines may register new schema versions after compatibility validation |
| `producer-service-account` | `READ` (subject-level) | Producers resolve schema IDs; cannot register or mutate schemas at runtime |
| `consumer-service-account` | `READ` (subject-level) | Consumers fetch schemas by ID; cannot register or mutate schemas at runtime |
| `analyst-account` | `READ` (global) | Read-only access for schema discovery |

**Security invariant:** No service running in production has `WRITE` access to the Schema Registry. Schema mutations are exclusively a CI/CD concern. This prevents a misconfigured or compromised producer from corrupting the schema contract for all downstream consumers.

---

## 8. Known Limitations & Future Work

| Item | Description | Priority |
|---|---|---|
| `created_at` as string | Should be `{"type": "long", "logicalType": "timestamp-millis"}` for Parquet efficiency | Medium |
| `total_price` as `double` | Should be `decimal(12,2)` for financial precision | High |
| `payment_method`/`status` as string | Should be Avro `enum` for encoding efficiency and validation | Low |
| Schema Registry HA | Single SR instance; no standby. Production requires SR cluster with `kafkastore.replication.factor=3` | High |
| `FULL_TRANSITIVE` for shared subjects | `BACKWARD` allows independent upgrades but does not protect against reading data two versions back | Medium |

---

*Related Documents: [TILL-DESIGN-001 High-Level Architecture](./01-high-level-architecture.md) | [TILL-DESIGN-003 Producer/Consumer Patterns](./03-producer-consumer-patterns.md)*
