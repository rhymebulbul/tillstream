# Phase 4 Jiras (Consumers & The DLQ)

**Goal:** Implement a polyglot architecture by writing our consumers in Python, demonstrating how Avro and the Schema Registry allow seamless data sharing between Go and Python. We will also implement a Dead Letter Queue (DLQ) pattern for fault tolerance.

## TILL-12: Implement Python Consumer for `orders` Topic
* **Description:** Create a Python service that connects to the Kafka cluster and the Schema Registry, consuming the highly-concurrent Avro messages produced by our Go service and deserializing them back into Python dictionaries.
* **Acceptance Criteria:**
  * Create a `consumers/` directory with a Python virtual environment or Dockerfile.
  * Use the `confluent-kafka[avro]` library to subscribe to the `orders` topic.
  * Verify the Python consumer successfully receives and prints the deserialized JSON data to the console.

## TILL-13: Implement DLQ (Dead Letter Queue) Pattern
* **Description:** Build resilience into the Python consumer by simulating a downstream failure (e.g., a simulated database timeout for a specific `store_id`) and routing those failed messages to a DLQ topic.
* **Acceptance Criteria:**
  * Introduce an intentional, simulated processing error in the consumer logic.
  * When the error is caught, produce the original message to a new Kafka topic named `orders-dlq`.
  * Verify that the main consumer continues processing subsequent messages without crashing.
