# Design Document 05: Observability, Data Contracts, & MLOps

**Author:** Staff Data Engineer
**Status:** Approved
**Last Updated:** August 2026

## 1. Executive Summary
A production data pipeline must guarantee data integrity and operational transparency. This document details the implementation of sub-millisecond telemetry tracking, infrastructure alerting, and real-time MLOps data quality constraints designed to protect downstream machine learning models from silent data corruption.

## 2. Infrastructure Observability

### 2.1 The Latency Tracking Fallacy
Relying solely on "Consumer Lag" (the absolute delta between the producer offset and consumer offset) is a flawed metric for user experience. A lag of 1,000 messages could mean 1 millisecond of delay (during a spike) or 10 minutes of delay (if processing is stalled).

### 2.2 True End-to-End Latency via Header Injection
1.  **Ingestion Timestamp:** The Golang Producer calculates `time.Now().UnixMilli()` immediately before the TCP network buffer flush and injects it into the Kafka Record Headers as `generation_time_ms`.
2.  **Delta Calculation:** The Python Consumer extracts the header, requests the current system clock time, and calculates the true delta.
3.  **Clock Drift Mitigation:** Assumes NTP (Network Time Protocol) synchronization across the cluster with drift bounds < 1ms.
4.  **Prometheus Histograms:** The latency is exposed via a `/metrics` endpoint to Prometheus, heavily bucketed (1ms, 5ms, 10ms, 25ms, etc.) to accurately calculate the p95 and p99 latency percentiles via PromQL (`histogram_quantile`).

### 2.3 Alerting Architecture
*   Prometheus executes periodic PromQL evaluations.
*   **Rule:** `sum(kafka_consumergroup_lag) > 500`. If sustained for 2 minutes, Prometheus fires an alert to Alertmanager, which handles deduplication, grouping, and paging the on-call engineer via PagerDuty.

## 3. MLOps: Defending the Data Lake

### 3.1 Real-Time Data Contracts (Great Expectations)
"Garbage In, Garbage Out." If an upstream microservice introduces a bug that causes `total_price` to become negative, downstream ML models will silently train on this data, degrading their predictive accuracy.
*   **Micro-Batch Enforcement:** Applying complex rules to single messages destroys throughput. The Python Consumer accumulates a micro-batch (e.g., 20 messages), converts it to a Pandas DataFrame, and executes a Great Expectations validation suite.
*   **Contract:** `expect_column_values_to_be_between('total_price', min_value=0)`.
*   **Quarantine Protocol:** If the suite fails, the *entire batch* is routed to an `orders-quarantine` topic, preventing pollution of the Iceberg Lakehouse.

### 3.2 Statistical Data Drift Detection (Evidently AI)
While Great Expectations catches immediate logical bugs, data drift happens slowly (e.g., a macro-economic shift causing users to spend 2x more over 6 months).
*   **Implementation:** An offline batch job (scheduled via Airflow) runs an Evidently AI script.
*   **Mechanism:** It pulls a "Reference" dataset (historical baseline) and a "Current" dataset (recent week) from Trino.
*   **Statistical Testing:** It applies tests (e.g., Kolmogorov-Smirnov test for numerical features, Jensen-Shannon divergence for categorical features) to detect distribution shifts.
*   **Remediation:** If the `DataDriftPreset` threshold is breached, an automated HTML report is generated and an alert is sent to the Data Science team to trigger an ML model retrain.
