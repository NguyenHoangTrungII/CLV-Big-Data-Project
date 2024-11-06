import pandas as pd
from sqlalchemy import create_engine

def save_data(data):
    # Kết nối đến PostgreSQL
    engine = create_engine('postgresql://postgres:admin@localhost:5432/product_recommendation')
    
    # Lưu DataFrame vào bảng 'Phone' trong cơ sở dữ liệu
    data.to_sql('Order', engine, if_exists='replace', index=False)
    
    print("Data stored in PostgreSQL")
