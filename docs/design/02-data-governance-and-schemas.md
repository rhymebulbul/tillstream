# Design Doc 02: Data Governance & Schema Evolution

## Motivation
In a distributed streaming architecture, producers and consumers are inherently decoupled. If a producer alters the data structure (e.g., renaming a field, changing a type, or removing a mandatory column), downstream consumers will instantly fail, causing a cascading pipeline outage. 

To solve this, TillStream employs strict data contracts via **Confluent Schema Registry** and **Apache Avro**.

## Architecture & Implementation

### 1. Apache Avro Serialization
Instead of sending raw, bloated JSON over the wire, the Golang Producer serializes all messages into binary Avro. 
*   **Efficiency:** Avro drastically reduces payload size, maximizing Kafka's throughput and minimizing network I/O.
*   **Strict Typing:** Every field is explicitly typed in a `.avsc` file.

### 2. Confluent Schema Registry
The Schema Registry sits alongside Kafka. Before the Go Producer can send a message, it must register its schema with the Registry. 
*   **The Magic Byte:** The producer prepends a 5-byte Confluent header to the binary payload:
    *   `Byte 0`: Magic Byte (always `0`).
    *   `Bytes 1-4`: The 32-bit Schema ID registered in the Registry.
*   **Dynamic Deserialization:** The Python consumer receives the binary, strips the first 5 bytes, uses the ID to query the Schema Registry, and perfectly deserializes the payload—even if it has never seen that schema before.

### 3. Schema Evolution & Compatibility
TillStream is configured to reject breaking changes (e.g., removing a required field).
*   **Conflict Prevention:** If a developer attempts to push a schema that violates backward compatibility, the Registry returns an `HTTP 409 Conflict`, physically preventing the poison pill from entering the stream.
*   **Forward & Backward Compatibility:** The system supports safe schema evolution. For example, adding a new field is permitted *only* if a `default` value is provided, ensuring older consumers don't crash when reading new messages.
