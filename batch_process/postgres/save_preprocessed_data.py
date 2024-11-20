import pandas as pd
from sqlalchemy import create_engine

from pyspark.sql.functions import col, regexp_replace, to_timestamp, hour, dayofweek, when
from pyspark.sql.types import IntegerType, FloatType

# def save_data(data):
#     # Kết nối đến PostgreSQL
#     engine = create_engine('postgresql://postgres:admin@localhost:5432/clv_datas')
    
#     data.to_sql('Order', engine, if_exists='replace', index=False)
    
#     print("Data stored in PostgreSQL")


import pandas as pd
from sqlalchemy import create_engine

def save_data_to_postgresql(data, table_name='"Order"'):
    """
    Lưu dữ liệu vào PostgreSQL.

    Args:
        data (DataFrame): Dữ liệu cần lưu.
        table_name (str): Tên bảng trong PostgreSQL.
    """
    try:

        print("data type:", type(data))
        # Kết nối đến PostgreSQL
        # jdbc_url = create_engine('postgresql://postgres:admin@localhost:5432/clv_datas')
        jdbc_url = "jdbc:postgresql://localhost:5432/clv_datas"  # Adjust database name and port
        properties = {
            'user': 'postgres',
            'password': '123',
            'driver': 'org.postgresql.Driver'
        }

        # Lưu dữ liệu vào bảng trong PostgreSQL, thay thế nếu bảng đã tồn tại
        data.write \
            .jdbc(url=jdbc_url, table=table_name, mode='overwrite', properties=properties)

        print(f"Data has been successfully stored in PostgreSQL table: {table_name}")
    except Exception as e:
        print(f"Error while saving data to PostgreSQL: {e}")

# Ví dụ sử dụng:
# data = pd.DataFrame(...)  # Dữ liệu đã làm sạch
# save_data_to_postgresql(data)
