# import pandas as pd
# from hdfs import InsecureClient
# from io import BytesIO

# # Step 1: Connect to HDFS and read the data
# def read_data_from_hdfs():
#     hdfs_host = 'localhost'
#     hdfs_port = 50070
#     hdfs_user = 'nhtrung'
#     file_path = '/batch-layer/raw_data.csv'

#     client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user=hdfs_user)
    
#     try:
#         # Read data from HDFS into a DataFrame
#         with client.read(file_path) as reader:
#             df = pd.read_csv(reader)
#         print("Data loaded from HDFS successfully.")
#         return df
#     except Exception as e:
#         print(f"Error reading data from HDFS: {e}")
#         return pd.DataFrame()

# # Step 2: Data Cleaning and Preprocessing
# def clean_and_process_data():

#     df = read_data_from_hdfs()

#     if df.empty:
#         print("No data to process.")
#         return df

#     # Convert 'InvoiceDate' to datetime format
#     df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')

#     # Drop rows where CustomerID is null
#     df_cleaned = df.dropna(subset=['CustomerID'])
#     df_cleaned['CustomerID'] = df_cleaned['CustomerID'].astype(str)

#     # Remove rows with non-positive quantities and prices
#     df_cleaned = df_cleaned[(df_cleaned['Quantity'] > 0) & (df_cleaned['UnitPrice'] > 0)]

#     # Display the cleaned data summary
#     print(f"Remaining rows after cleaning: {df_cleaned.shape[0]}")
#     print("Summary Statistics:")
#     print(df_cleaned.describe())

#     return df_cleaned


# # Main function
# def main():
#     print("Starting data processing...")
#     cleaned_data = clean_and_process_data()
    
#     if not cleaned_data.empty:
#         print("Data processing completed successfully.")
#     else:
#         print("Data processing failed or no data to process.")

# # Run the main function if the script is executed directly
# if __name__ == "__main__":
#     main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, hour, dayofweek, when, unix_timestamp
from pyspark.sql.types import IntegerType, FloatType

from batch_process.postgres.save_preprocessed_data import save_data_to_postgresql

def create_spark_session():
    """
    Create a SparkSession.
    """
    spark = SparkSession.builder \
        .appName("Data Cleaning and Transformation") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    return spark


def read_data_from_hdfs(spark, file_path):
    """
    Read data from HDFS.
    Args:
        spark: SparkSession instance.
        file_path: Path to the file in HDFS.

    Returns:
        DataFrame containing the data.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


def clean_and_transform_data(df):
    """
    Clean and process the data.
    Args:
        df: Original DataFrame.

    Returns:
        Processed DataFrame.
    """
    # Remove null values
    df = df.dropna(subset=["InvoiceDate", "Quantity", "UnitPrice"])


    # Clean and configure columns
    df = df.withColumn("InvoiceDate", regexp_replace(col("InvoiceDate"), "[^0-9\-: ]", ""))
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))

    df = df \
        .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
        .withColumn("UnitPrice", col("UnitPrice").cast(FloatType())) \
        .withColumn("hour", hour(col("InvoiceDate"))) \
        .withColumn("dayofweek", dayofweek(col("InvoiceDate"))) \
        .withColumn("weekend", when(dayofweek(col("InvoiceDate")).isin(6, 7), 1).otherwise(0)) \
        .withColumn("Revenue", col("Quantity") * col("UnitPrice")) \
        .withColumn("InvoiceDate", unix_timestamp(col("InvoiceDate")).cast("double"))

    return df


def save_data_to_hdfs(df, output_path):
    """
    Save the processed data to HDFS.
    Args:
        df: Processed DataFrame.
        output_path: Path to save the file in HDFS.
    """
    df.write.csv(output_path, header=True, mode="overwrite")


def main():
    """
    Main program for data processing.
    """
    # Create SparkSession
    spark = create_spark_session()

    # File paths for input/output in HDFS
    input_path = "hdfs://localhost:9000/batch-layer/raw_data.csv"
    output_path = "hdfs://localhost:9000/path/to/processed_data.csv"

    # Read data from HDFS
    print("Reading data from HDFS...")
    df = read_data_from_hdfs(spark, input_path)

    # Clean and process the data
    print("Cleaning and processing the data...")
    processed_df = clean_and_transform_data(df)

    # Display results
    print("Processed data:")
    processed_df.show(10)

    # Save the processed data to HDFS
    print("Saving the processed data to HDFS...")
    # save_data_to_hdfs(processed_df, output_path)
    save_data_to_postgresql(processed_df)

    print("Processing complete!")


if __name__ == "__main__":
    main()
