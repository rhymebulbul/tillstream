# 🔒 Security & Compliance Architecture

TillStream is designed with enterprise-grade security constraints in mind, addressing both traditional data governance (GDPR/CCPA) and modern Agentic AI vulnerabilities.

## 1. PII Cryptographic Masking (GDPR/CCPA Compliance)
To ensure compliance with global data privacy regulations, the streaming pipeline guarantees that no raw Personally Identifiable Information (PII) enters the Apache Iceberg Lakehouse.
- **In-Stream Masking:** Before records are flushed to the downstream data lake, the Validation Consumer intercepts sensitive fields (such as `customer_id`).
- **One-Way Hashing:** These fields are irreversibly hashed using SHA-256 and a secure salt. This allows data analysts to track unique user behavior (via the consistent hash) without exposing the underlying identity to unauthorized personnel.

## 2. Zero-Trust Agentic Sandboxing (Prompt Injection Defense)
The DLQ Resolver uses LLMs to dynamically generate and execute Python code (`exec()`). This introduces a massive vector for Remote Code Execution (RCE) if a malicious actor injects a prompt into a Kafka payload.
- **Restricted Globals:** The `exec()` sandbox explicitly strips Python's `__builtins__`, neutralizing functions like `eval()`, `open()`, or unauthorized imports.
- **Module Denylist:** The sandbox environment strictly forbids importing OS-level modules (e.g., `os`, `sys`, `subprocess`), physically preventing the LLM from executing shell commands or reading environment variables (like API keys) even if a Prompt Injection attack is highly successful.

## 3. Kafka Transport Layer Security (SASL/SCRAM)
To ensure Zero-Trust networking within the data center, the data streaming layer is architected to reject unauthenticated connections.
- **Authentication:** Producers and Consumers authenticate with the KRaft brokers using robust SASL/SCRAM authentication.
- **Authorization (ACLs):** Granular Access Control Lists (ACLs) ensure that the Golang Producer can only `WRITE` to the `orders` topic, physically preventing compromised producer instances from reading downstream proprietary data.
