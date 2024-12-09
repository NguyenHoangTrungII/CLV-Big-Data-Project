import pandas as pd
import json
from kafka import KafkaProducer
# from confluent_kafka import Producer
import time

# Hàm chuyển đổi `Timestamp` sang chuỗi
def serialize_data(data):
    if isinstance(data, pd.Timestamp):
        return data.strftime('%Y-%m-%d %H:%M:%S')  # Chuyển sang định dạng phù hợp với Spark
    elif isinstance(data, dict):
        return {key: serialize_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_data(item) for item in data]
    return data

# Hàm để xử lý và chuẩn bị dữ liệu, đảm bảo không có giá trị NaN hoặc None
def prepare_message(row):
          # Handle NaN and replace None with appropriate defaults
    row = row.fillna('') 
    row['Quantity'] = row.get('Quantity', 0)
    row['UnitPrice'] = row.get('UnitPrice', 0)
    row['Description'] = row.get('Description', '')
    row['StockCode']= row.get('StockCode', '')
    row['Invoice']= row.get('Invoice', 0)

    # ... handle other columns accordingly ...
    return row.to_dict()


# # Thiết lập Kafka producer
producer = KafkaProducer(
    # bootstrap_servers='localhost:9093',
    bootstrap_servers='172.27.254.108:9093',  
    value_serializer=lambda v: json.dumps(serialize_data(v)).encode('utf-8')
)

def send_data():
    # Tải dữ liệu từ file Excel
    df = pd.read_excel('/home/nhtrung/CLV-Big-Data-Project/data/raw/Online_Retail.xlsx', engine='openpyxl')    
    print("Sending data to Kafka...")
    
    for index, row in df.iterrows():
        message = prepare_message(row)
        
        try:
            producer.send('CLV_system_nhtrung', value=message)
            print(f"Sent message {index}: {message}")
            time.sleep(5)  # Thêm độ trễ trước khi gửi dòng tiếp theo
        except Exception as e:
            print(f"Error in row {index}: {e}")
    
    producer.flush()
    print("All data was sent successfully.")

if __name__ == "__main__":
    send_data()
