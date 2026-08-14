# Design Doc 05: Observability & MLOps

## Motivation
A data platform operating blindly is a liability. TillStream implements comprehensive infrastructure observability and proactive data quality enforcement (MLOps) to catch both performance bottlenecks and silent data corruption.

## 1. End-to-End Latency Tracking
Simply measuring "Consumer Lag" (the number of unread messages in Kafka) does not accurately reflect user experience.
*   **Timestamp Injection:** The Go Producer captures the exact `time.Now().UnixMilli()` right before network transmission and injects it into the Kafka Message **Headers**.
*   **Delta Calculation:** The Python Consumer extracts this header, compares it to the current time, and calculates the exact latency in milliseconds.
*   **Prometheus Histograms:** This precise timing is exposed via a local Prometheus HTTP server. Prometheus scrapes this data and builds latency distribution buckets, proving that TillStream achieves sub-10ms end-to-end processing speeds.

## 2. Infrastructure Alerting
*   **Kafka Exporter:** A dedicated exporter translates Kafka's internal JMX metrics into Prometheus format.
*   **Alertmanager:** Alerting rules are configured. If Consumer Lag exceeds 500 messages (indicating the consumer has stalled or crashed), Prometheus transitions the alert to a `FIRING` state.

## 3. MLOps: Real-Time Data Contracts (Great Expectations)
If upstream engineers deploy a bug that generates negative prices, downstream ML models will silently train on poisoned data.
*   **Micro-Batch Validation:** The Python consumer accumulates small batches of data and wraps them in a Pandas DataFrame.
*   **Contract Enforcement:** Using **Great Expectations**, the consumer validates the batch against strict business rules (e.g., `total_price >= 0`).
*   **Quarantine:** If the contract is violated, the batch is immediately rejected and routed to an `orders-quarantine` topic, actively preventing bad data from entering the Iceberg Lakehouse.

## 4. MLOps: Data Drift (Evidently AI)
While contracts catch immediate bugs, statistical drift happens slowly (e.g., macro-economic inflation gradually shifting average order prices).
*   **Distribution Testing:** An **Evidently AI** script periodically compares the "Current" data distribution against a historical "Reference" baseline.
*   **Automated Reporting:** If the statistical distribution shifts significantly, an HTML report is generated and an alert is triggered to notify Data Scientists to retrain their models.
