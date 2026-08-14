# Phase 2 Jiras (Topic & Partition Design)

**Goal:** Understand and manage Kafka partition strategies, specifically dealing with tenant volume skew (hot partitions) and configuring topic retention.

## TILL-08: Verify Topic Partitioning and Volume Skew in Grafana
* **Description:** Ensure that the `orders` and `payments` topics are being partitioned correctly by `tenant_id`, and that the volume skew simulated by the producer is visible in monitoring tools.
* **Acceptance Criteria:**
  * Login to Grafana (provided in Phase 0).
  * Build or import a dashboard visualizing messages-per-second per partition for the `orders` topic.
  * Verify that a small number of partitions (handling the "Flagship" tenants) are receiving 80% of the traffic, creating hot partitions.

## TILL-09: Reconfigure Topic Retention and Partitions
* **Description:** Write administrative scripts to alter the configuration of the live topics to relieve pressure from the hot partitions and enforce data retention rules.
* **Acceptance Criteria:**
  * Write a script (e.g., `infra/scripts/update_topics.sh`) using the Kafka CLI tools to increase partition counts for the `orders` and `payments` topics dynamically.
  * Apply a specific time-based retention policy (e.g., 7 days) and size-based retention policy (e.g., 5GB) to the topics to demonstrate production-ready configuration.
