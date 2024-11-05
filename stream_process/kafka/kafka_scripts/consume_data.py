from kafka import KafkaConsumer

# Tạo Kafka consumer để nhận dữ liệu từ topic
consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='my-group'
)

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
