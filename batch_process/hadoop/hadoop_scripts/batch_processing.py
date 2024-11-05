import pandas as pd
from hdfs import InsecureClient
from kafka import KafkaConsumer
import json
import ast

def store_data_in_hdfs(transaction_data):
    # Chỉ lấy các trường cần thiết từ JSON
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

    # Tạo DataFrame từ dữ liệu đã chuẩn bị
    transaction_df = pd.DataFrame([data])

    # Kết nối với HDFS
    hdfs_host = 'localhost'
    hdfs_port = 50070
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')

    # Tạo thư mục /batch-layer nếu chưa tồn tại
    try:
        if not client.status('/batch-layer'):
            client.makedirs('/batch-layer')
    except Exception as e:
        print(f"Lỗi khi kiểm tra hoặc tạo thư mục: {e}")

    # Kiểm tra xem file đã tồn tại hay chưa
    try:
        if not client.status('/batch-layer/raw_data.csv'):
            # Nếu file không tồn tại, ghi dữ liệu mới
            transaction_df.to_csv('/batch-layer/raw_data.csv', index=False, header=True)
        else:
            # Nếu file đã tồn tại, đọc dữ liệu hiện có và kết hợp với dữ liệu mới
            with client.read('/batch-layer/raw_data.csv') as reader:
                existing_df = pd.read_csv(reader)
            combined_df = pd.concat([existing_df, transaction_df], ignore_index=True)
            with client.write('/batch-layer/raw_data.csv', overwrite=True) as writer:
                combined_df.to_csv(writer, index=False, header=True)
    
    except Exception as e:
        print(f"Lỗi khi lưu dữ liệu vào HDFS: {e}")

def consume_hdfs():
    # Cấu hình Kafka
    bootstrap_servers = 'localhost:9092'
    topic = 'clv_nhtrung'

    # Tạo Kafka consumer
    consumer = KafkaConsumer(topic,
                             group_id='my_consumer_group',
                             auto_offset_reset='latest',
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))

    # Lặp qua các message nhận được từ Kafka
    for message in consumer:
        try:
            data = message.value
            data = ast.literal_eval(data)  # Chuyển đổi chuỗi thành dict
            store_data_in_hdfs(data)
            print("Dữ liệu đã được lưu vào HDFS:", data)
            print("-------------------")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Lỗi khi giải mã dữ liệu: {e}")
            continue

if __name__ == "__main__":
    consume_hdfs()
