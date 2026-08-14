# Phase 6 Jiras (Advanced Observability & Alerting)

**Goal:** Upgrade the existing Grafana stack from basic visualization to active alerting, simulating a true production on-call environment.

## TILL-16: Implement Consumer Lag Alerting Rules
* **Description:** Configure Prometheus Alertmanager to fire alerts when a consumer group falls too far behind the producer.
* **Acceptance Criteria:**
  * Define a Prometheus recording/alerting rule in `infra/prometheus/alert.rules`.
  * Trigger the alert by intentionally pausing the Python consumer container while the producer runs.
  * Verify the alert fires in the Prometheus/Grafana UI.

## TILL-17: Build End-to-End Latency Dashboard
* **Description:** Create a custom Grafana dashboard tracking the time delta between event generation and consumer processing.
* **Acceptance Criteria:**
  * Ensure the producer embeds a generation timestamp in the payload headers.
  * Update the Python consumer to calculate latency (current time - generation time).
  * Expose this metric to Prometheus and build a latency percentile graph in Grafana.
