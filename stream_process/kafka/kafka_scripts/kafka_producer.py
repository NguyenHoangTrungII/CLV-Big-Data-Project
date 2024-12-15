from kafka import KafkaProducer
import json

def create_kafka_producer(bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8')):
    """
    Create a Kafka producer with specified configuration.

    Parameters:
        bootstrap_servers (str): Kafka bootstrap servers, e.g., 'localhost:9092'.
        value_serializer (callable): A function to serialize message values. Defaults to JSON serialization.

    Returns:
        KafkaProducer: A configured KafkaProducer instance.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=value_serializer
    )
    return producer
