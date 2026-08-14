# Phase 3 Jiras (Schema Evolution)

**Goal:** Understand the Confluent Schema Registry's compatibility rules by intentionally breaking the schema, and then deploying a correct, backward-compatible schema evolution.

## TILL-10: Intentionally Break Schema Compatibility
* **Description:** Modify the Go data model and the Avro schema for the `orders` topic in a way that violates the default `BACKWARD` compatibility rule (e.g., removing a required field or changing a field's data type).
* **Acceptance Criteria:**
  * Update the `orders` schema string in the Go producer code.
  * Rebuild the producer container.
  * Observe the Schema Registry rejecting the new schema with an HTTP 409 Conflict error, proving our data platform is protecting us from bad data.

## TILL-11: Deploy a Backward-Compatible Evolution
* **Description:** Fix the schema evolution by adding a new, optional field (e.g., `loyalty_points_earned`) with a valid default value, ensuring it passes the compatibility checks.
* **Acceptance Criteria:**
  * Revert the breaking change and apply the compatible change.
  * Rebuild the producer container.
  * Verify that the Schema Registry successfully registers `version 2` of the `orders` schema.
  * Verify that the producer seamlessly continues streaming data using the evolved schema.
