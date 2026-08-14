import struct
import json
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:29092'})

# Fake JSON payload where total_price is a string instead of float (POISON PILL)
bad_data = {
    "order_id": "poison-order-1",
    "tenant_id": "TENANT_FLAGSHIP_1",
    "store_id": "TENANT_FLAGSHIP_1_STORE_1",
    "customer_id": "cust-1",
    "loyalty_points": 100,
    "total_price": "50.5", # POISON! Avro schema expects a float, not a string
    "created_at": "2026-08-14T00:00:00Z"
}

# 5 byte magic header for schema ID 1
magic_header = struct.pack('>bI', 0, 1) 
payload = magic_header + json.dumps(bad_data).encode('utf-8')

producer.produce('orders', value=payload, key=b"TENANT_FLAGSHIP_1")
producer.flush()
print("💀 Poison pill injected! The Python consumer will crash on this and route to DLQ.")
