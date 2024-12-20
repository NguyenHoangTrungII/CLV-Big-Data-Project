import pandas as pd
# from sqlalchemy import create_engine
# from pyspark.sql.functions import col, regexp_replace, to_timestamp, hour, dayofweek, when
# from pyspark.sql.types import IntegerType, FloatType

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

def save_to_postgres(df, table_name, db_url, db_properties, mode="append"):
    """
    Save a Spark DataFrame to a PostgreSQL database.

    Parameters:
        df (DataFrame): The Spark DataFrame to save.
        table_name (str): The target table name in the PostgreSQL database.
        db_url (str): The JDBC URL for connecting to the PostgreSQL database.
        db_properties (dict): A dictionary containing PostgreSQL connection properties (user, password, driver).
        mode (str): The save mode, default is "append". Other options include "overwrite" and "ignore".

    Returns:
        None
    """
    try:
        df.write.jdbc(url=db_url, table=table_name, mode=mode, properties=db_properties)
        print(f"DataFrame has been successfully saved to table '{table_name}' in PostgreSQL.")
    except Exception as e:
        print(f"Error saving DataFrame to PostgreSQL: {e}")

