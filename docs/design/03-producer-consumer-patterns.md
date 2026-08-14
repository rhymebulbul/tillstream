# TillStream: Producer/Consumer Patterns & Resilience Engineering

| Field | Value |
|---|---|
| **Document ID** | TILL-DESIGN-003 |
| **Status** | Approved |
| **Author** | Staff Data Engineer |
| **Last Updated** | August 2026 |
| **Reviewers** | Principal Engineer (Platform), Staff SRE |

---

## 1. Executive Summary

This document defines the distributed systems patterns that govern TillStream's ingestion and consumption layers. The central engineering challenges are:

1. **Traffic skew:** 80% of message volume originates from two "Flagship" tenants. Keyed partitioning by `tenant_id` is necessary for ordering guarantees but concentrates I/O onto specific brokers and partitions.
2. **Exactly-once delivery:** The ingestion path must not double-count transactions or drop events, even under transient network failures and broker leader re-elections.
3. **Head-of-line blocking:** A single unprocessable message on a Kafka partition stalls all subsequent messages in that partition for all consumers in the group. This is the fundamental availability threat in any Kafka consumer implementation.

The patterns described here — idempotent producers, deterministic key partitioning, salted key fan-out, and Dead Letter Queue (DLQ) routing — collectively ensure that no single failure mode results in permanent data loss or pipeline stall.

---

## 2. Producer Architecture

### 2.1 Event Generation & Traffic Skew

The Go producer generates correlated `Order` and `Payment` event pairs using the `generator.GenerateOrderFlow()` function. The tenant distribution deliberately implements a **Pareto (80/20) skew** to simulate real-world retail B2B SaaS traffic patterns:

From [`producers/internal/generator/generator.go`](../../producers/internal/generator/generator.go):

```go
var tenants = []string{
    "TENANT_FLAGSHIP_1", "TENANT_FLAGSHIP_2",       // Flagship: indices 0, 1
    "TENANT_LOCAL_1", "TENANT_LOCAL_2", "TENANT_LOCAL_3", // SMB: indices 2, 3, 4
}

func GetRandomTenantID() string {
    if rand.Intn(100) < 80 {
        return tenants[rand.Intn(2)]  // 80% Flagship traffic
    }
    return tenants[2+rand.Intn(3)]   // 20% SMB traffic
}
```

**Consequence:** At 100k msg/sec, each Flagship tenant generates ~40k msg/sec. With 50 partitions and `murmur2` hashing:

```
partition("TENANT_FLAGSHIP_1") = murmur2("TENANT_FLAGSHIP_1") % 50 = N (fixed)
```

All 40k msg/sec from `TENANT_FLAGSHIP_1` land on partition N. This is a **hot partition** by definition. See §2.3 for the mitigation strategy.

### 2.2 Deterministic Partitioning by Tenant ID

All messages are keyed by `tenant_id`. This is not an optional optimization — it is a **correctness requirement**. The downstream Python consumer implements state machine transitions for wallet balance updates. These transitions are only valid when processed in the order the events were produced. Kafka guarantees message ordering only within a single partition. Therefore, all events for a given `tenant_id` must land on the same partition.

From [`producers/internal/kafka/producer.go`](../../producers/internal/kafka/producer.go):

```go
err = tp.producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{
        Topic:     &topic,
        Partition: kafka.PartitionAny,  // Kafka chooses partition based on key hash
    },
    Key:   []byte(key),   // key = tenant_id — drives murmur2 partition assignment
    Value: finalPayload,
    Headers: []kafka.Header{
        {Key: "generation_time_ms", Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))},
    },
}, deliveryChan)
```

`kafka.PartitionAny` combined with a non-nil `Key` triggers Kafka's default `murmur2` partitioner. The partition assignment is deterministic and stable as long as the number of partitions does not change.

**Warning:** Adding partitions to an existing topic changes the hash space and breaks the ordering guarantee for keys that hash to new partition boundaries. Topic repartitioning requires a coordinated migration with consumer replay from the checkpoint.

### 2.3 Hot Partition Mitigation: The Salted Key Pattern

When a Flagship tenant's throughput exceeds the I/O capacity of a single Kafka partition (~10 MB/s on standard broker hardware), the broker throttles the producer for that partition, creating cascading latency across all co-located partitions on the same broker.

**The Salted Key Pattern** splits a single tenant's traffic across multiple partitions by appending a random bucket suffix to the message key:

```
// Without salting (current)
key = "TENANT_FLAGSHIP_1"
→ All 40k msg/sec → partition N

// With salting (Phase 2 mitigation)
salt_factor = 4  // Pre-defined split factor for Flagship tenants
bucket = rand.Intn(salt_factor)  // 0, 1, 2, or 3
key = fmt.Sprintf("TENANT_FLAGSHIP_1_%d", bucket)
→ 10k msg/sec each → partitions N, M, P, Q
```

**Critical consequence — K-Way Merge requirement:** After salting, events for `TENANT_FLAGSHIP_1` are spread across 4 partitions. Global ordering across the tenant is lost at the partition level. The downstream consumer must implement a **K-Way Merge** to reconstruct chronological ordering:

1. Consumer reads from all 4 partitions simultaneously, maintaining a priority queue (min-heap) keyed by `created_at` timestamp.
2. The head of the priority queue (minimum timestamp) is processed first.
3. Consumer commits offsets per partition independently.

This adds O(K log K) overhead per event at the consumer, where K = number of salt buckets. K must be chosen carefully: too large increases consumer complexity; too small leaves hot partitions unresolved.

**Status:** The salted key pattern is **not yet implemented**. The current single-partition-per-tenant design is appropriate for the current load profile. Flagship tenant throughput is monitored via per-partition byte rate in Grafana. The salting threshold trigger is > 8 MB/s sustained for > 5 minutes on a single partition.

### 2.4 Exactly-Once Semantics (EOS) & Idempotent Production

**The problem:** Under network instability, a producer's `Produce()` call may time out waiting for the broker ACK. The producer retries. If the original message was already written to the partition log by the broker (but the ACK was lost in transit), the retry creates a **duplicate message**.

For POS transactions, a duplicate `Order` event would cause double-counting of revenue, double-accrual of loyalty points, and incorrect wallet balance state transitions.

**The solution:** Kafka's idempotent producer assigns a `Producer ID (PID)` and a monotonically increasing `sequence number` to each message. The broker maintains a deduplication window (keyed by `PID, partition, sequence_number`). On retry, the broker detects the duplicate sequence number and discards the message without writing to the log, returning a success ACK to the producer.

**Configuration requirements (production):**

```go
// In kafka.NewProducer()
&kafka.ConfigMap{
    "bootstrap.servers":  brokerURL,
    "enable.idempotence": true,   // Required for EOS
    "acks":               "all",  // Required for EOS: all ISR replicas must ACK
    "retries":            10,     // High retry count; sequence number handles dedup
    "retry.backoff.ms":   500,
}
```

> **Current implementation gap:** The existing `NewTillProducer()` in `producer.go` does not explicitly set `enable.idempotence=true`. This is a P1 configuration gap — idempotence must be enabled before this service processes real financial transactions.

### 2.5 In-Memory Buffering During Broker Unavailability

Validated in [INC-001](../incidents/INC-001-broker-failure.md): When the Kafka broker was killed, the Go producer did not crash. Instead, it buffered generated events in its internal `queue.buffering.max.messages` ring buffer (defaults to 100,000 messages in `confluent-kafka-go`).

Upon broker restart, the KRaft controller re-elected a partition leader within seconds. The producer re-established its TCP connection and immediately flushed the in-memory buffer. **No events were lost. No upstream HTTP requests were failed.**

This behavior is a consequence of Kafka's asynchronous producer model: `Produce()` enqueues to an internal buffer; actual transmission occurs on a background I/O thread. The application thread never blocks on broker availability.

```go
// Current: synchronous ACK wait via deliveryChan (blocks per message)
// This negates the buffering advantage under load — refactor target
e := <-deliveryChan
m := e.(*kafka.Message)
if m.TopicPartition.Error != nil {
    return m.TopicPartition.Error
}
```

> **Refactor note:** The current `deliveryChan` pattern makes each `ProduceMessage()` call synchronous — it blocks until the broker ACK is received. This is safe but eliminates pipelining throughput gains. For 100k msg/sec targets, this should be replaced with a fire-and-forget pattern with a separate goroutine draining the `Events()` channel for error handling.

---

## 3. Consumer Architecture

### 3.1 Consumer Group Isolation

TillStream deploys two independent consumer groups against the `orders` topic:

| Consumer Group | Implementation | Purpose | Offset Store |
|---|---|---|---|
| `python-orders-consumer` | `consumers/main.py` | Stream processing: data quality enforcement, DLQ routing, latency telemetry | Kafka `__consumer_offsets` topic |
| `spark-orders-consumer` | `lakehouse/spark/stream_to_iceberg.py` | Lakehouse ingestion: Kafka → Iceberg | MinIO WAL at `s3a://lakehouse/checkpoints/orders` |

Each group maintains its own committed offset per partition. A failure in the Spark job does not affect the Python consumer's position, and vice versa. This is the fundamental isolation guarantee of Kafka's consumer group model.

### 3.2 Micro-Batch Accumulation

The Python consumer does not process each message in isolation. It accumulates messages into a micro-batch of 20 records before applying Great Expectations data quality validation:

```python
batch_records = []
batch_msgs = []

while True:
    msg = consumer.poll(1.0)
    # ... deserialize msg → record ...
    batch_records.append(record)
    batch_msgs.append((msg, payload, record))

    if len(batch_records) >= 20:
        # Apply Great Expectations on the entire batch
        df = pd.DataFrame(batch_records)
        ge_df = ge.from_pandas(df)
        # ... validation ...
        batch_records.clear()
        batch_msgs.clear()
```

**Rationale:** Running Great Expectations validation on a single-record basis is prohibitively expensive. Constructing a `pandas.DataFrame` per record introduces `O(n)` overhead for each validation suite call. Batching amortizes the DataFrame construction cost across 20 records while keeping the quarantine blast radius bounded (at most 20 messages are quarantined per violation, not 20,000).

**Tradeoff:** A batch of 20 messages spans multiple Kafka `poll()` calls. If the process crashes mid-batch after processing 15 of 20 messages but before committing offsets, all 20 messages are reprocessed on restart (at-least-once semantics). For the current use case (logging, telemetry, DLQ routing), duplicate processing is acceptable. For exactly-once semantics to an external OLTP store, Kafka Transactions would be required.

### 3.3 Offset Management

The consumer uses `auto.offset.reset=earliest`, meaning a fresh consumer group (no previously committed offset) starts reading from the beginning of the topic's retention window. In production, this is safe because:
1. The 7-day retention window provides a full replay buffer.
2. A new consumer group deployed during an incident can backfill all missed events.
3. Great Expectations validation on historical data is idempotent — duplicate quarantine routing is harmless.

`auto.commit` is implicitly enabled (Confluent's default). **This means offset commits happen in the background, independent of message processing.** Under a crash scenario, up to `auto.commit.interval.ms` (default 5000ms) of processed messages may be reprocessed. This is acceptable for the current consumer's side-effect profile (Prometheus metrics, Kafka topic writes).

---

## 4. Dead Letter Queue (DLQ) Architecture

### 4.1 The Head-of-Line Blocking Problem

Kafka partitions are ordered append-only logs. A consumer reads partition messages sequentially. If message at offset N is unprocessable (e.g., corrupted bytes, schema violation, downstream DB offline), the consumer cannot skip to offset N+1 without explicitly committing offset N. If the consumer crashes instead of committing, it restarts and replays offset N indefinitely.

This is **head-of-line blocking:** one bad message at position N blocks all subsequent messages (N+1, N+2, ...) from being processed until N is handled.

### 4.2 Failure Taxonomy

TillStream distinguishes two fundamentally different failure types requiring different remediation strategies:

| Failure Type | Examples | Retry Appropriate? | DLQ Topic |
|---|---|---|---|
| **Poison Pill (Permanent)** | Unknown magic byte; schema violation; corrupted Avro bytes | ❌ No — message is structurally invalid | `orders-dlq` |
| **Transient (Recoverable)** | Downstream DB timeout; network flap; HTTP 429 rate limit | ✅ Yes — with exponential backoff | `orders-dlq` (with retry metadata) |
| **Contract Violation (Batch)** | `total_price < 0`; `loyalty_points < 0` | ❌ No — upstream data bug | `orders-quarantine` |

The distinction matters for automated remediation: transient failures in the DLQ can be retried by a replay agent without human intervention; poison pills require manual triage or an LLM-based automated repair agent (Phase 9).

### 4.3 DLQ Envelope Format

Messages routed to `orders-dlq` are wrapped in an envelope that preserves the original message context:

```python
# Simulated transient failure (TENANT_FLAGSHIP_1 → 20% DB timeout rate)
if r.get('tenant_id') == 'TENANT_FLAGSHIP_1' and random.random() < 0.2:
    dlq_producer.produce(
        'orders-dlq',
        value=p,          # Original wire-format payload (magic byte + avro binary)
        key=m.key()       # Original partition key (tenant_id) preserved
    )
    continue              # Commit this offset; do not crash
```

**Production enhancement required:** The current DLQ envelope only carries the original payload and key. A production DLQ envelope must also include:

```python
import json, traceback

dlq_envelope = {
    "original_payload_b64": base64.b64encode(p).decode(),
    "original_key": m.key().decode(),
    "original_topic": m.topic(),
    "original_partition": m.partition(),
    "original_offset": m.offset(),
    "original_timestamp_ms": m.timestamp()[1],
    "error_type": "TRANSIENT_DB_TIMEOUT",
    "error_message": str(exception),
    "stack_trace": traceback.format_exc(),
    "retry_count": 0,
    "dlq_enqueued_at_ms": int(time.time() * 1000),
}
dlq_producer.produce('orders-dlq', value=json.dumps(dlq_envelope).encode(), key=m.key())
```

### 4.4 DLQ Consumer & Replay

The DLQ `orders-dlq` is itself a Kafka topic. A separate DLQ consumer (not yet implemented) must:

1. Read from `orders-dlq`.
2. Parse the envelope to extract `error_type`, `retry_count`, and `original_payload`.
3. For `TRANSIENT` failures: re-produce the `original_payload` to `orders` with exponential backoff (max retries = 5).
4. For `POISON_PILL` failures: route to `orders-dead-archive` (permanent storage) and alert on-call.
5. Increment `retry_count` in the envelope; if `retry_count >= max_retries`, escalate to dead archive.

**Phase 9 — Agentic DLQ Resolver:** An LLM-based agent monitors `orders-dlq` for `POISON_PILL` messages caused by schema mismatches. The agent reads the Schema Registry, generates a Python transformation script to coerce the malformed payload into the registered schema, executes the script in a sandboxed environment, validates the output, and replays the corrected message to `orders`. This eliminates the manual triage step for the most common class of poison pill failures.

### 4.5 Quarantine Topic vs. DLQ: Design Distinction

| Property | `orders-dlq` | `orders-quarantine` |
|---|---|---|
| **Trigger** | Per-message routing failure (DB error, corrupt bytes) | Batch-level Great Expectations contract violation |
| **Granularity** | Single message | Batch of 20 messages |
| **Retry semantics** | Retryable (transient) or archive (poison pill) | Not retried; requires upstream bug fix |
| **Downstream** | DLQ resolver / replay agent | Data quality team alert; upstream incident |

Both topics use the same key schema as the source `orders` topic (`tenant_id`) to preserve partition locality for any future replay operations.

---

## 5. Consumer Group Rebalancing

When a consumer worker is added, removed, or crashes, Kafka triggers a **consumer group rebalance**. During a rebalance:

1. All consumers in the group stop processing (`max.poll.interval.ms` deadline).
2. The group coordinator redistributes partitions.
3. Processing resumes from the last committed offset for each partition.

**Current configuration risk:** The Python consumer's main processing loop calls `consumer.poll(1.0)` — a 1-second poll timeout. Great Expectations validation on a batch of 20 records must complete in well under `max.poll.interval.ms` (default 5 minutes) or the consumer will be evicted from the group. For the current workload this is not a concern, but as validation suites grow in complexity (e.g., adding cross-field statistical tests), this timeout must be monitored and tuned.

**Recommended production addition:**

```python
consumer_conf = {
    'bootstrap.servers': kafka_broker,
    'group.id': 'python-orders-consumer',
    'auto.offset.reset': 'earliest',
    'max.poll.interval.ms': 300000,    # 5 minutes (default; tune down as needed)
    'session.timeout.ms': 45000,       # 45 seconds
    'heartbeat.interval.ms': 15000,    # 15 seconds
}
```

---

*Related Documents: [TILL-DESIGN-001 High-Level Architecture](./01-high-level-architecture.md) | [TILL-DESIGN-002 Data Governance](./02-data-governance-and-schemas.md) | [TILL-DESIGN-004 Lakehouse Architecture](./04-lakehouse-and-analytics.md)*
