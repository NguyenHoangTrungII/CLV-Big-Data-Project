# import pandas as pd
# import json
# from kafka import KafkaProducer
# # from confluent_kafka import Producer
# import time

# # Hàm chuyển đổi `Timestamp` sang chuỗi
# def serialize_data(data):
#     if isinstance(data, pd.Timestamp):
#         return data.strftime('%Y-%m-%d %H:%M:%S')  # Chuyển sang định dạng phù hợp với Spark
#     elif isinstance(data, dict):
#         return {key: serialize_data(value) for key, value in data.items()}
#     elif isinstance(data, list):
#         return [serialize_data(item) for item in data]
#     return data

# # Hàm để xử lý và chuẩn bị dữ liệu, đảm bảo không có giá trị NaN hoặc None
# def prepare_message(row):
#           # Handle NaN and replace None with appropriate defaults
#     row = row.fillna('') 
#     row['Quantity'] = row.get('Quantity', 0)
#     row['UnitPrice'] = row.get('UnitPrice', 0)
#     row['Description'] = row.get('Description', '')
#     row['StockCode']= row.get('StockCode', '')
#     row['Invoice']= row.get('Invoice', 0)

#     # ... handle other columns accordingly ...
#     return row.to_dict()


# # Thiết lập Kafka producer
# producer = KafkaProducer(
#     # bootstrap_servers='localhost:9093',
#     bootstrap_servers='172.27.179.20:9093',  
#     value_serializer=lambda v: json.dumps(serialize_data(v)).encode('utf-8')
# )

# def send_data():
#     # Tải dữ liệu từ file Excel
#     df = pd.read_excel('/home/nhtrung/CLV-Big-Data-Project/data/raw/Online_Retail.xlsx', engine='openpyxl')    
#     print("Sending data to Kafka...")
    
#     for index, row in df.iterrows():
#         message = prepare_message(row)
        
#         try:
#             producer.send('CLV_system_nhtrung', value=message)
#             print(f"Sent message {index}: {message}")
#             time.sleep(20)  # Thêm độ trễ trước khi gửi dòng tiếp theo
#         except Exception as e:
#             print(f"Error in row {index}: {e}")
    
#     producer.flush()
#     print("All data was sent successfully.")

# if __name__ == "__main__":
#     send_data()

import pandas as pd
import json
from kafka import KafkaProducer
from pydantic import BaseModel
from typing import List
import time

# Định nghĩa schema cho dữ liệu
class Item(BaseModel):
    StockCode: str
    Description: str
    Quantity: int
    UnitPrice: float


class Invoice(BaseModel):
    InvoiceNo: str
    InvoiceDate: str
    CustomerID: str
    Country: str
    Items: List[Item]


# Hàm để xử lý dữ liệu từng dòng và nhóm
def group_and_format(df):
    df['StockCode'] = df['StockCode'].astype(str)

    grouped_data = []
    
    # Duyệt qua từng Invoice để gom nhóm
    for (invoice, invoice_date, customer_id, country), group in df.groupby(
        ['InvoiceNo', 'InvoiceDate', 'CustomerID', 'Country']
    ):
        items = []
        for _, row in group.iterrows():
            # Tạo các item theo schema
            item = Item(
                StockCode=row['StockCode'],
                Description=row['Description'],
                Quantity=int(row['Quantity']),
                UnitPrice=float(row['UnitPrice'])
            )
            items.append(item)

        # Tạo invoice theo schema
        invoice_data = Invoice(
            InvoiceNo=invoice,
            InvoiceDate=invoice_date,
            CustomerID=customer_id,
            Country=country,
            Items=items
        )
        grouped_data.append(invoice_data.dict())  # Convert to dictionary for Kafka
    
    return grouped_data

def preprocess_data(df):
    # Loại bỏ khoảng trắng trong tên cột
    df.columns = df.columns.str.strip()

    # Kiểm tra các cột cần thiết
    required_columns = ['InvoiceNo', 'InvoiceDate', 'CustomerID', 'Country', 'StockCode', 'Description', 'Quantity', 'UnitPrice']
    for col in required_columns:
        if col not in df.columns:
            raise KeyError(f"Missing required column: {col}")

    # Loại bỏ các dòng bị thiếu dữ liệu
    df = df.dropna(subset=required_columns)

    # Chuyển đổi kiểu dữ liệu
    df['InvoiceNo'] = df['InvoiceNo'].astype(str)
    df['CustomerID'] = df['CustomerID'].astype(str)
    df['StockCode'] = df['StockCode'].astype(str)
    df['Quantity'] = df['Quantity'].astype(int)
    df['UnitPrice'] = df['UnitPrice'].astype(float)

    # Chuyển đổi InvoiceDate sang chuỗi định dạng
    df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df['Description'] = df['Description'].astype(str)

    return df


def send_data():
    # Đọc dữ liệu từ Excel
    df = pd.read_excel(
        '/home/nhtrung/CLV-Big-Data-Project/data/raw/Online_Retail.xlsx',
        engine='openpyxl'
    )

    # Điền giá trị mặc định nếu thiếu
    df.fillna({
        'CustomerID': '',
        'InvoiceDate': '',
        'Country': '',
        'StockCode': '',
        'Description': '',
        'Quantity': 0,
        'UnitPrice': 0.0
    }, inplace=True)

    # print("Grouping and formatting data before sending to Kafka...")
    df = preprocess_data(df)
    grouped_data = group_and_format(df)
    
    # Thiết lập Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='172.31.56.16:9093',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Gửi dữ liệu đến Kafka
    for index, data in enumerate(grouped_data):
        try:
            producer.send('CLV_system_nhtrung', value=data)
            # print(f"Sent message {index}: {data}")
            time.sleep(20)  # Thêm độ trễ nếu cần
        except Exception as e:
            print(f"Error in sending data {index}: {e}")
    
    producer.flush()
    print("All data was sent successfully.")


if __name__ == "__main__":
    send_data()
