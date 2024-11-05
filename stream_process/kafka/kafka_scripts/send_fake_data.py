import threading
import pandas as pd
import json
from kafka import KafkaProducer, KafkaConsumer
import time
import subprocess

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(serialize_data(v)).encode('utf-8')
)

# Function to serialize data
def serialize_data(data):
    if isinstance(data, pd.Timestamp):
        return data.isoformat()
    elif isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_data(item) for item in data]
    return data

# Producer function to send data
def send_data():
    df = pd.read_excel('./data/raw/Online_Retail.xlsx')
    print("Sending data to Kafka...")
    for index, row in df.iterrows():
        message = row.to_dict()
        try:
            producer.send('pclv_nhtrung', value=message)
            print(f"Sent message {index}: {message}")
            time.sleep(1)
        except Exception as e:
            print(f"Error in row {index}: {e}")
    producer.flush()
    print("All data sent to Kafka successfully.")

# Kafka Consumer setup
consumer = KafkaConsumer(
    'product_recommendations_nhtrung',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Function to save data to Hadoop
def save_to_hadoop(data):
    with open('/tmp/consumer_data.json', 'w') as file:
        json.dump(data, file)
    subprocess.run(['hdfs', 'dfs', '-put', '/tmp/consumer_data.json', '/user/hadoop/product_data/'])

# Consumer function to consume data
def consume_data():
    print("Consuming data from Kafka...")
    for message in consumer:
        print(f"Consumed data: {message.value}")
        save_to_hadoop(message.value)

# Running producer and consumer in parallel using threads
producer_thread = threading.Thread(target=send_data)
consumer_thread = threading.Thread(target=consume_data)

if __name__ == "__main__":
    producer_thread.start()
    consumer_thread.start()
    producer_thread.join()
    consumer_thread.join()

