import os
import struct
import json
import io
import fastavro
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient

def main():
    # 1. Connect to Schema Registry
    sr_conf = {'url': os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")}
    sr_client = SchemaRegistryClient(sr_conf)
    
    # Cache to avoid repeatedly fetching schemas from the registry
    schema_cache = {}

    # 2. Connect to Kafka
    consumer_conf = {
        'bootstrap.servers': os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092"),
        'group.id': 'python-orders-consumer',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['orders'])

    print("🚀 Starting dynamic Python Avro Consumer... waiting for messages.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            payload = msg.value()
            if not payload:
                continue
                
            # Confluent Avro Wire Format:
            # Byte 0: Magic Byte (always 0)
            # Bytes 1-4: Schema ID (4-byte integer)
            # Bytes 5+: Avro payload
            magic, schema_id = struct.unpack('>bI', payload[:5])
            
            if magic != 0:
                print("Error: Unknown magic byte!")
                continue

            # Dynamically fetch the schema if we haven't seen this ID before
            if schema_id not in schema_cache:
                print(f"Fetching Schema ID {schema_id} from Registry...")
                schema = sr_client.get_schema(schema_id)
                schema_cache[schema_id] = fastavro.parse_schema(json.loads(schema.schema_str))
                
            # Deserialize the Avro binary payload dynamically
            avro_data = payload[5:]
            bytes_reader = io.BytesIO(avro_data)
            record = fastavro.schemaless_reader(bytes_reader, schema_cache[schema_id])
            
            print(f"✅ Processed Order: {record.get('order_id')} | Tenant: {record.get('tenant_id')} | Price: ${record.get('total_price')} | Loyalty Pts: {record.get('loyalty_points')}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Consumer shut down.")

if __name__ == '__main__':
    main()
