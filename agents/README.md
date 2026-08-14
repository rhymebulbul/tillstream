# TillStream Agentic DLQ Resolver

This directory contains the autonomous AI agent capable of monitoring the Dead Letter Queue (DLQ), reading schema-corrupted binary payloads, prompting an LLM to generate a Python data-wrangling fix, and executing that fix in a sandbox to replay the message to the main pipeline.

## Running the Agent Locally (Ollama)

Because local Python environments can sometimes have issues building C-extensions (`confluent-kafka` and `fastavro`) on newer Python versions without `python3-dev` installed, the most reliable way to run this agent is using Docker with **Host Networking**. 

Host networking allows the Docker container to seamlessly connect to your local Kafka broker (`localhost:9092`) and your local Ollama daemon (`localhost:11434`) without complex bridge routing.

### 1. Start the DLQ Agent

Open a terminal at the root of the `tillstream` repository and run:

```bash
docker run -it --rm \
  --network host \
  -v $(pwd):/app -w /app \
  -e OLLAMA_MODEL="qwen2.5-coder:3b" \
  -e KAFKA_BOOTSTRAP_SERVERS="localhost:9092" \
  -e SCHEMA_REGISTRY_URL="http://localhost:8081" \
  python:3.10-slim \
  bash -c "pip install -r agents/requirements.txt && python3 agents/dlq_resolver.py"
```

*Note: You can change the `OLLAMA_MODEL` environment variable to any model you have downloaded locally via `ollama list`.*

### 2. Inject a Poison Pill

Wait until Terminal 1 finishes installing dependencies and says it is "polling 'orders-dlq' for failed messages." 

Open a **second terminal tab** (also at the root of the repository) and run the following command. This will intentionally craft a malformed JSON payload and inject it directly into Kafka to crash the Python consumer.

```bash
docker run -it --rm \
  --network host \
  -v $(pwd):/app -w /app \
  -e KAFKA_BOOTSTRAP_SERVERS="localhost:9092" \
  python:3.10-slim \
  bash -c "pip install confluent-kafka && python3 infra/scripts/inject_poison_pill.py"
```

### 3. Watch the Magic

Switch back to your first terminal. You will see the agent:
1. Detect the malformed message (skipping any transient DB timeouts).
2. Fetch the Avro Schema from the Schema Registry.
3. Prompt your local Ollama model to generate the Python fix.
4. Execute the fix, successfully coerce the data types, and replay it to the main `orders` topic!

### Screenshots

> The agent receives a corrupted message, fetches the Avro schema, and prompts the local Ollama model to generate a Python repair function:

![Agent code generation via Ollama](../docs/assets/agent-healing-codegen.png)

> The generated code is executed in a sandbox — data types are coerced, the payload is re-serialized, and replayed to the main topic:

![Agent self-healing success](../docs/assets/agent-healing-success.png)
