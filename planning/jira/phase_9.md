# Phase 9 Jiras (The "God Tier" Agentic DLQ)

**Goal:** Combine traditional Data Engineering with Agentic AI to build a self-healing data platform.

## TILL-22: Scaffold LLM Agent Service for DLQ Monitoring
* **Description:** Create a new Python service that connects to an LLM provider and constantly polls the `orders-dlq` topic.
* **Acceptance Criteria:**
  * Service successfully reads a failed message and its associated error stack trace.
  * The service prompts the LLM with the error, the raw payload, and the expected Schema Registry definition.

## TILL-23: Implement Autonomous Payload Mutator and Replay
* **Description:** Empower the LLM to write a sandboxed Python transformation script to fix the broken message and replay it.
* **Acceptance Criteria:**
  * The LLM successfully generates a Python function that coerces the malformed data into the correct schema (e.g., casting a string to an integer).
  * The service executes the function in a secure sandbox, validates it against the Schema Registry, and pushes the fixed message back to the main `orders` topic.
