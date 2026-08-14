import os
import struct
import json
import io
import fastavro
import random
import time
import pandas as pd
import great_expectations as ge
from prometheus_client import start_http_server, Histogram
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient

LATENCY_HISTOGRAM = Histogram(
    'tillstream_message_latency_ms',
    'End-to-End Latency in ms',
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]
)

def main():
    # Start Prometheus Metrics Server
    start_http_server(8000)
    
    # 1. Connect to Schema Registry
    sr_conf = {'url': os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")}
    sr_client = SchemaRegistryClient(sr_conf)
    
    # Cache to avoid repeatedly fetching schemas from the registry
    schema_cache = {}

    # 2. Connect to Kafka (Consumer & DLQ Producer)
    kafka_broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    
    dlq_producer = Producer({'bootstrap.servers': kafka_broker})
    
    consumer_conf = {
        'bootstrap.servers': kafka_broker,
        'group.id': 'python-orders-consumer',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['orders'])

    print("🚀 Starting dynamic Python Avro Consumer... waiting for messages.")

    batch_records = []
    batch_msgs = []

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
            # Accumulate into micro-batches for Great Expectations
            batch_records.append(record)
            batch_msgs.append((msg, payload, record))
            
            if len(batch_records) >= 20:
                # 5. Enforce Data Quality with Great Expectations (TILL-20)
                df = pd.DataFrame(batch_records)
                ge_df = ge.from_pandas(df)
                
                res_price = ge_df.expect_column_values_to_be_between('total_price', min_value=0)
                res_loyalty = ge_df.expect_column_values_to_be_between('loyalty_points', min_value=0)
                
                if not res_price.success or not res_loyalty.success:
                    print(f"🚨 DATA CONTRACT VIOLATION! Routing {len(batch_records)} records to Quarantine...")
                    for m, p, r in batch_msgs:
                        dlq_producer.produce('orders-quarantine', value=p, key=m.key())
                else:
                    # Process clean batch
                    for m, p, r in batch_msgs:
                        # 3. Simulate a flaky downstream database for our high-volume tenant
                        if r.get('tenant_id') == 'TENANT_FLAGSHIP_1' and random.random() < 0.2:
                            print(f"❌ DB TIMEOUT! Failed to process Order {r.get('order_id')}. Routing to DLQ...")
                            dlq_producer.produce('orders-dlq', value=p, key=m.key())
                            continue
                            
                        # 4. Calculate End-to-End Latency if header exists
                        if m.headers():
                            for k, v in m.headers():
                                if k == 'generation_time_ms':
                                    try:
                                        latency_ms = int(time.time() * 1000) - int(v.decode('utf-8'))
                                        if latency_ms >= 0:
                                            LATENCY_HISTOGRAM.observe(latency_ms)
                                    except ValueError:
                                        pass
                                        
                        print(f"✅ Processed Order: {r.get('order_id')} | Tenant: {r.get('tenant_id')} | Price: ${r.get('total_price')} | Loyalty Pts: {r.get('loyalty_points')}")
                        
                dlq_producer.poll(0)
                batch_records.clear()
                batch_msgs.clear()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        dlq_producer.flush()
        print("Consumer shut down.")

if __name__ == '__main__':
    main()
