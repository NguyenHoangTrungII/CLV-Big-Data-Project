import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql.functions import col, regexp_replace, to_timestamp, hour, dayofweek, when
from pyspark.sql.types import IntegerType, FloatType

from batch_process.postgres.postgres_config.config import jdbc_url, properties

def save_data_to_postgresql(data, table_name='"Order"'):
    """
    Save data to PostgreSQL.

    Args:
        data (DataFrame): The data to be saved.
        table_name (str): The name of the table in PostgreSQL.
    """
    try:
        print("data type:", type(data))
        # Save the data into a table in PostgreSQL, replacing the table if it already exists
        data.write \
            .jdbc(url=jdbc_url, table=table_name, mode='overwrite', properties=properties)
        print(f"Data has been successfully stored in PostgreSQL table: {table_name}")
    except Exception as e:
        print(f"Error while saving data to PostgreSQL: {e}")
