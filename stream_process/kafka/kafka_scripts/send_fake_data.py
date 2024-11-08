import pandas as pd
import json
from kafka import KafkaProducer
import time

# Hàm chuyển đổi `Timestamp` sang chuỗi
def serialize_data(data):
    if isinstance(data, pd.Timestamp):
        return data.isoformat()  # Chuyển sang định dạng chuỗi ISO 8601
    elif isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_data(item) for item in data]
    return data

# Thiết lập Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(serialize_data(v)).encode('utf-8')
)

def send_data():
    # Tải dữ liệu từ file Excel
    df = pd.read_excel('./data/raw/Online_Retail.xlsx')
    
    print("Sending data to Kafka...")
    for index, row in df.iterrows():
        message = row.to_dict()
        
        try:
            producer.send('CLV_system_nhtrung', value=message)
            print(f"Sent message {index}: {message}")
            time.sleep(5)  # Thêm độ trễ trước khi gửi dòng tiếp theo
        except Exception as e:
            print(f"Error in row {index}: {e}")
    
    producer.flush()
    print("All data was sent successly.")

if __name__ == "__main__":
    send_data()
