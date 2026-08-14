# Phase 1 Jiras (Producers & Realistic Event Generation)

**Goal:** Build a highly concurrent Golang application to simulate Point-of-Sale (POS) systems across multiple retail tenants generating Avro-serialized events into Kafka.

## TILL-04: Initialize Golang Producer Module & Skeleton
* **Description:** Set up the Go project structure within the `/producers` directory.
* **Acceptance Criteria:**
  * `go mod init` executed.
  * Basic `main.go` entry point established.
  * Project structure created (e.g., `/cmd`, `/internal/generator`, `/internal/kafka`).

## TILL-05: Create POS Event Data Models & Faker Integration
* **Description:** Define the Golang structs for the 4 core event types: `Order`, `Payment`, `Refund`, and `InventoryAdjustment`. Integrate a fake data generator (like `brianvoe/gofakeit` or `go-faker/faker`) to populate these structs realistically.
* **Acceptance Criteria:**
  * Structs defined for all 4 events.
  * Helper function created that returns randomized but logically consistent mock data (e.g., a payment amount matches an order amount).

## TILL-06: Implement Tenant Volume Skew Algorithm
* **Description:** To test Kafka partition hot-spotting later, we need simulated tenants to generate vastly different volumes of data.
* **Acceptance Criteria:**
  * Create a predefined list of "Tenants" (e.g., 100 tenant IDs).
  * Implement a weighted randomization loop where a few "Flagship" tenants generate 80% of the events, while the rest generate 20%.
  * Goroutines used to simulate concurrent tenant activity.

## TILL-07: Implement Kafka Producer Client & Avro Serialization
* **Description:** Integrate a Kafka client (`confluent-kafka-go` or `twmb/franz-go`) and serialize the generated Go structs into Avro format before publishing to Kafka.
* **Acceptance Criteria:**
  * Successfully connects to the local Kafka broker (`localhost:9092`).
  * Serializes payloads using schemas (which will be defined in Phase 3) or basic Avro libraries.
  * Publishes messages reliably to respective topics (`orders`, `payments`, `refunds`, `inventory-adjustments`).
