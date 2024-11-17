import pandas as pd
from sqlalchemy import create_engine

def save_data(data):
    # Kết nối đến PostgreSQL
    engine = create_engine('postgresql://postgres:admin@localhost:5432/clv_datas')
    
    data.to_sql('Order', engine, if_exists='replace', index=False)
    
    print("Data stored in PostgreSQL")
