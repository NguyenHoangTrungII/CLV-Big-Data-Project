import pandas as pd
from hdfs import InsecureClient
from kafka import KafkaConsumer
import json
import ast
from io import BytesIO

from stream_process.kafka.kafka_scripts.kafka_consumer import create_kafka_consumer

def store_data_in_hdfs(transaction_data):
    # Extract the necessary fields from the JSON
    data = {
        'InvoiceNo': transaction_data.get('InvoiceNo', None),
        'StockCode': transaction_data.get('StockCode', None),
        'Description': transaction_data.get('Description', None),
        'Quantity': transaction_data.get('Quantity', None),
        'InvoiceDate': transaction_data.get('InvoiceDate', None),
        'UnitPrice': transaction_data.get('UnitPrice', None),
        'CustomerID': transaction_data.get('CustomerID', None),
        'Country': transaction_data.get('Country', None)
    }

    # Create a DataFrame from the prepared data
    transaction_df = pd.DataFrame([data])

    # Connect to HDFS
    hdfs_host = 'localhost'
    hdfs_port = 50070
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='root')

    # Ensure the directory exists
    try:
        client.makedirs('/batch-layer')  # Create directory if it does not exist
    except Exception as e:
        print(f"Error creating directory: {e}")

    # Check if the file exists in HDFS
    file_path = '/batch-layer/raw_data.csv'
    try:
        if client.status(file_path):  # If file exists
            # Read existing data and append new data
            with client.read(file_path) as reader:
                existing_df = pd.read_csv(reader)
            combined_df = pd.concat([existing_df, transaction_df], ignore_index=True)
        else:
            combined_df = transaction_df
    except Exception as e:
        # If the file doesn't exist, we'll start fresh
        combined_df = transaction_df

    # Write the combined DataFrame back to HDFS
    try:
        with client.write(file_path, overwrite=True) as writer:
            # Create BytesIO buffer to store CSV data
            output = BytesIO()
            # Writing DataFrame as bytes into BytesIO buffer
            combined_df.to_csv(output, index=False, header=True, encoding='utf-8')
            
            # Ensure the BytesIO buffer is in the correct state (seek to the start)
            output.seek(0)

            # Debugging: Check the type and content of the buffer before writing
            content = output.getvalue()
            print(f"Data type to write to HDFS: {type(content)}")  # Should print: <class 'bytes'>
            print(f"First 100 bytes to be written: {content[:100]}")  # Print the first 100 bytes to check if it's correct

            # Ensure we're writing the data as bytes, not a string
            writer.write(content)
        print("Data has been saved to HDFS:", data)
    except Exception as e:
        print(f"Error saving data to HDFS: {e}")

def consume_hdfs():
    # # Configure Kafka
    # bootstrap_servers = 'localhost:9093'
    # topic = 'CLV_system_nhtrung'

    # # Create Kafka consumer
    # consumer = KafkaConsumer(
    #     topic,
    #     group_id='my_consumer_group',
    #     auto_offset_reset='latest',
    #     bootstrap_servers=bootstrap_servers,
    #     value_deserializer=lambda x: x.decode('utf-8'))

    # Configure Kafka
    bootstrap_servers = 'localhost:9093'
    topic = 'CLV_system_nhtrung'
    group_id = 'my_consumer_group'

    # Create Kafka consumer
    consumer = create_kafka_consumer(bootstrap_servers, topic, group_id)

    # Loop through the received messages from Kafka
    for message in consumer:
        try:
            data = message.value
            data = ast.literal_eval(data)  # Convert string to dict
            store_data_in_hdfs(data)
            print("Data has been saved to HDFS:", data)
            print("-------------------")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error decoding data: {e}")
            continue

if __name__ == "__main__":
    consume_hdfs()
