# Phase 8 Jiras (MLOps & Data Quality)

**Goal:** Introduce Data Quality constraints and Data Drift monitoring to protect downstream Machine Learning models from silent data failures.

## TILL-20: Integrate Great Expectations for Data Quality
* **Description:** Embed Great Expectations into the Python consumer to enforce business rules on the incoming data stream before it reaches the Lakehouse.
* **Acceptance Criteria:**
  * Define an Expectations Suite (e.g., `total_price > 0`, `loyalty_points >= 0`).
  * If a batch of data fails the expectations, route it to an `orders-quarantine` topic.

## TILL-21: Set up Data Drift Monitoring with Evidently AI
* **Description:** Periodically run statistical tests to detect if the simulated retail data distributions change significantly.
* **Acceptance Criteria:**
  * Implement an Evidently AI report generation script.
  * Trigger an alert if the distribution of `total_price` significantly shifts, simulating a pricing bug or macro-economic shift.
