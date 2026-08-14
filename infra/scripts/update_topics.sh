#!/bin/bash

# Exit on error
set -e

echo "==> Updating partition counts to 5 for 'orders' and 'payments' topics..."
# Note: You can only increase partition counts in Kafka, not decrease them.
docker exec broker kafka-topics --bootstrap-server localhost:9092 --alter --topic orders --partitions 5
docker exec broker kafka-topics --bootstrap-server localhost:9092 --alter --topic payments --partitions 5

echo "==> Applying retention policies (7 days / 5GB) to 'orders' and 'payments' topics..."
# retention.ms=604800000 (7 days)
# retention.bytes=5368709120 (5 GB)
docker exec broker kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name orders --add-config retention.ms=604800000,retention.bytes=5368709120
docker exec broker kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name payments --add-config retention.ms=604800000,retention.bytes=5368709120

echo "==> Topics updated successfully. Describing the 'orders' topic:"
docker exec broker kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
