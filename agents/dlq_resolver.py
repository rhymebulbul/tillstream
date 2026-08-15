import os
import json
import struct
import io
import time
import fastavro
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
import google.generativeai as genai

def main():
    # Setup Kafka Connections
    kafka_broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    dlq_consumer = Consumer({
        'bootstrap.servers': kafka_broker,
        'group.id': 'agentic-dlq-resolver-v2',
        'auto.offset.reset': 'latest'
    })
    dlq_consumer.subscribe(['orders-dlq'])

    main_producer = Producer({'bootstrap.servers': kafka_broker})
    sr_client = SchemaRegistryClient({'url': os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})

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
            
            if error_header == "Unknown Error":
                print("🔄 Detected transient DB timeout. Skipping payload mutation...")
                continue
                        
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
            try:
                exec(code, globals(), local_env)
                fix_func = local_env['fix_payload']
                
                # Execute the LLM's generated function
                fixed_dict = fix_func(payload)
            except Exception as e:
                print(f"❌ AI Hallucination or Sandbox Error! Failed to execute generated code: {e}")
                continue
            
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

if __name__ == "__main__":
    main()
