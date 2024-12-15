from kafka import KafkaConsumer

def create_kafka_consumer(bootstrap_servers, topic, group_id, auto_offset_reset='latest', value_deserializer=lambda x: x.decode('utf-8')):
    """
    Create a Kafka consumer with specified configuration.

    Parameters:
        bootstrap_servers (str): Kafka bootstrap servers, e.g., 'localhost:9092'.
        topic (str): The Kafka topic to subscribe to.
        group_id (str): The consumer group ID.
        auto_offset_reset (str): Where to start reading messages if offset is not available ('earliest' or 'latest').
        value_deserializer (callable): A function to deserialize message values. Defaults to UTF-8 decoding.

    Returns:
        KafkaConsumer: A configured KafkaConsumer instance.
    """
    consumer = KafkaConsumer(
        topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=value_deserializer
    )
    return consumer
