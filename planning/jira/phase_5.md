# Phase 5 Jiras (Failure Injection & Incident Response)

**Goal:** Prove the platform's resilience and document operational competency by intentionally introducing chaotic failures into the Kafka cluster and practicing incident response.

## TILL-14: Inject Broker Failure & Observe Rebalancing
* **Description:** Simulate a complete hardware failure of a Kafka broker while the producer and consumer are actively running.
* **Acceptance Criteria:**
  * Use Docker commands to suddenly kill the Kafka broker container mid-stream.
  * Monitor the Grafana dashboards to observe partition rebalancing and consumer lag spikes.
  * Restart the broker and verify the cluster heals and lag drains to zero.

## TILL-15: Write Incident Postmortem Document
* **Description:** Write a professional FAANG-style incident postmortem based on the failure injected in TILL-14.
* **Acceptance Criteria:**
  * Create `docs/incidents/INC-001-broker-failure.md`.
  * Include a timeline of events, root cause analysis (simulated), impact assessment, and remediation steps.
