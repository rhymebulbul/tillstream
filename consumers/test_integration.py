import time
import pytest
from testcontainers.kafka import KafkaContainer

@pytest.fixture(scope="module")
def kafka_container():
    # Spin up an ephemeral Kafka container
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        yield kafka

def test_kafka_connectivity_and_produce(kafka_container):
    """
    Integration Test: Proves that our python code can successfully
    connect to an actual Kafka broker and produce a message.
    """
    from confluent_kafka import Producer, Consumer
    
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    # 1. Produce a test poison pill message
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    test_topic = "orders-dlq-test"
    producer.produce(test_topic, value=b'\x00\x00\x00\x00\x01{"bad": "data"}', key=b'test-key')
    producer.flush()
    
    # 2. Consume the message
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([test_topic])
    
    # Poll for the message
    msg = consumer.poll(5.0)
    
    assert msg is not None
    assert msg.error() is None
    assert msg.value() == b'\x00\x00\x00\x00\x01{"bad": "data"}'
    
    consumer.close()
