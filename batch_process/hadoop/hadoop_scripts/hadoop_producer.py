import pandas as pd
from hdfs import InsecureClient
from io import BytesIO

def read_data_from_hdfs():
    hdfs_host = 'localhost'
    hdfs_port = 50070
    hdfs_user = 'hadoop'
    file_path = '/batch-layer/raw_data.csv'

    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user=hdfs_user)
    
    try:
        # Read data from HDFS into a DataFrame
        with client.read(file_path) as reader:
            df = pd.read_csv(reader)
        print("Data loaded from HDFS successfully.")
        return df
    except Exception as e:
        print(f"Error reading data from HDFS: {e}")
        return pd.DataFrame()
    
