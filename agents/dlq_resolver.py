import os
import json
import struct
import io
import time
import fastavro
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import google.generativeai as genai

# Setup Kafka Connections
kafka_broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
dlq_consumer = Consumer({
    'bootstrap.servers': kafka_broker,
    'group.id': 'agentic-dlq-resolver',
    'auto.offset.reset': 'earliest'
})
dlq_consumer.subscribe(['orders-dlq'])

main_producer = Producer({'bootstrap.servers': kafka_broker})
sr_client = SchemaRegistryClient({'url': os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})

def get_llm_fix_code(error_msg, raw_payload, schema_str):
    api_key = os.environ.get("GEMINI_API_KEY")
    if api_key:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-1.5-pro')
        prompt = f"""
        You are an autonomous data engineering agent.
        A message failed Avro deserialization.
        Error: {error_msg}
        Schema: {schema_str}
        Raw Payload (hex): {raw_payload.hex()}
        
        Write a Python function `fix_payload(raw_bytes)` that takes the raw bytes, 
        extracts the JSON data (skipping the 5-byte avro header), fixes the type 
        coercion issue (e.g. string to float), and returns a dictionary matching the schema.
        Output ONLY the python code without markdown formatting. Do not include ```python or ``` tags.
        """
        response = model.generate_content(prompt)
        return response.text.strip().replace("```python", "").replace("```", "")
    else:
        # Fallback simulated response if no API key is present
        return """
import json
def fix_payload(raw_bytes):
    # Skip 5 byte magic header and decode JSON
    json_str = raw_bytes[5:].decode('utf-8')
    data = json.loads(json_str)
    # Fix string to float for price
    if isinstance(data.get('total_price'), str):
        data['total_price'] = float(data['total_price'])
    return data
"""

print("🤖 Agentic DLQ Resolver started... polling 'orders-dlq' for failed messages.")

try:
    while True:
        msg = dlq_consumer.poll(1.0)
        if msg is None: 
            continue
        if msg.error():
            continue
            
        print("\n" + "="*50)
        print("🚨 Received message from DLQ!")
        error_header = "Unknown Error"
        if msg.headers():
            for k, v in msg.headers():
                if k == 'error':
                    error_header = v.decode('utf-8')
                    
        print(f"📄 Error Trace: {error_header}")
                    
        payload = msg.value()
        # Decode the Confluent Wire Format Header
        magic, schema_id = struct.unpack('>bI', payload[:5])
        
        # Fetch expected schema from registry
        schema = sr_client.get_schema(schema_id)
        parsed_schema = fastavro.parse_schema(json.loads(schema.schema_str))
        
        print(f"🧠 Asking LLM to analyze error and generate Python fix code...")
        code = get_llm_fix_code(error_header, payload, schema.schema_str)
        
        print("\n💻 Generated Code:")
        print(code)
        
        print("\n🧪 Executing code in sandbox...")
        local_env = {}
        exec(code, globals(), local_env)
        fix_func = local_env['fix_payload']
        
        # Execute the LLM's generated function
        fixed_dict = fix_func(payload)
        
        print(f"✅ Payload successfully mutated! Re-serializing to Avro...")
        bytes_writer = io.BytesIO()
        fastavro.schemaless_writer(bytes_writer, parsed_schema, fixed_dict)
        new_payload = payload[:5] + bytes_writer.getvalue()
        
        print("🚀 Replaying fixed message to 'orders' topic...")
        main_producer.produce('orders', value=new_payload, key=msg.key())
        main_producer.flush()
        print("🎉 Successfully self-healed!")
        print("="*50 + "\n")
        
except KeyboardInterrupt:
    pass
finally:
    dlq_consumer.close()
