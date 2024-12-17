import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, from_json, to_timestamp, hour, dayofweek, when, pandas_udf, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from tensorflow import keras
import numpy as np
import pandas as pd
import threading
from pyspark.sql.functions import lit
from happybase import ConnectionPool

from stream_process.hbase.hbase_scripts.hbase_consumer import insert_data_to_hbase, connect_to_hbase
from batch_process.spark.spark_scripts.spark_processing import process_streaming_features,clean_and_transform_data, write_to_hbase, transform_kafka_data_to_dataframe


# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Centralized HBase connection configuration
HBASE_CONFIG = {
    'host': 'localhost',  # Replace with actual HBase host
    'port': 9090,  # Default Thrift port, adjust if different
    'timeout': 10000  # Increased timeout in milliseconds
}
HBASE_POOL_SIZE = 5

# Create connection pool with more robust configuration
try:
    connection_pool = ConnectionPool(
        size=HBASE_POOL_SIZE, 
        host=HBASE_CONFIG['host'], 
        port=HBASE_CONFIG['port'], 
        timeout=HBASE_CONFIG['timeout']
    )
    logger.info(f"Successfully created connection pool with size {HBASE_POOL_SIZE}")
except Exception as pool_err:
    logger.error(f"Failed to create connection pool: {pool_err}")



# Kafka configuration
KAFKA_TOPIC = 'CLV_system_nhtrung'
KAFKA_BOOTSTRAP_SERVERS = '172.27.179.20:9093'

# Load the model ONCE (outside any threads)
model = None
try:
    model = keras.models.load_model('/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras', compile=False)
    logger.info("Model loaded successfully.")
except Exception as e:
    logger.error(f"Error loading model: {e}")
    exit(1)

# Schema for incoming Kafka data
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", FloatType(), True),
    StructField("CustomerID", FloatType(), True),
    StructField("Country", StringType(), True)
])

def preprocess_data(df):
    #All preprocessing is done in Spark now
    df = df.withColumn("InvoiceDate", regexp_replace(col("InvoiceDate"), "[^0-9\-: ]", ""))
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("Quantity", col("Quantity").cast(IntegerType())) \
           .withColumn("UnitPrice", col("UnitPrice").cast(FloatType())) \
           .withColumn("hour", hour(col("InvoiceDate"))) \
           .withColumn("dayofweek", dayofweek(col("InvoiceDate"))) \
           .withColumn("weekend", when(dayofweek(col("InvoiceDate")).isin(6, 7), 1).otherwise(0)) \
           .withColumn("Revenue", col("Quantity") * col("UnitPrice"))\
           \
           .withColumn("hour1", hour(col("InvoiceDate"))) \
           .withColumn("hour2", hour(col("InvoiceDate"))) \
           .withColumn("hour3", hour(col("InvoiceDate"))) \
           .withColumn("hour4", hour(col("InvoiceDate"))) \
           .withColumn("InvoiceDate", unix_timestamp(col("InvoiceDate")).cast("double"))

    #Select only necessary columns for prediction.
    # df = df.select("InvoiceNo", "CustomerID", "Quantity", "UnitPrice", "InvoiceDate", "Revenue", "hour","hour", "hour", "dayofweek", "weekend")
    # pandas_df = df.toPandas()

    df = df.select("InvoiceNo", "CustomerID", "Quantity", "UnitPrice", "InvoiceDate", "Revenue")

    
    return df.toPandas()

def register_udf(spark):
    @pandas_udf("double")
    def predict_udf(input_data_values):

        global model

        try:
            predictions = model.predict(input_data_values)
        except Exception as e:
            print(f"Error in predict {e}")

        return predictions
        # return pd.Series(predictions.flatten())
    return predict_udf

def connect_and_save_to_hbase(df_with_predictions):
    print("Starting to consume...") 
    # Connect to HBase
    connection = connect_to_hbase()
    
    if not connection:
        print("Error: Unable to connect to HBase. Exiting.")
        return

    insert_data_to_hbase(connection, df_with_predictions)

    print("Sucess insert into hbase") 

def process_batch(batch_df, batch_id, predict_udf):
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty!")
        return
        
    batch_df = transform_kafka_data_to_dataframe(batch_df)

    # Xử lý batch
    try:
        # processed_df_before =[]
        processed_df_before = clean_and_transform_data(batch_df)
        processed_df = process_streaming_features(processed_df_before)
    except Exception as e:
        logger.error(f"Error during pre-data for prediction: {e}")

   # Thực hiện dự đoán với mô hình ML
    try:
        features = processed_df.toPandas()
        # features = processed_df_before.toPandas()

        df_with_predictions = processed_df_before

        print("Features (Pandas DataFrame) Info:")
        print(features.info())  # In thông tin chi tiết
        print("Features Head:")
        print(features.head())  # In một vài dòng đầu tiên        
        predictions = model.predict(features)

        prediction_value = float(predictions[0][0])

        # df_with_predictions["CLV_Prediction"] = predictions[0][0] 
        df_with_predictions = df_with_predictions.withColumn(
            "CLV_Prediction", lit(prediction_value)
        )

        # Log kết quả
        # print("Predicted Batch Data:")
        # print(df_with_predictions['InvoiceDate'])

       
        # spark = batch_df.sql_ctx.sparkSession  # Lấy SparkSession từ batch_df
        
        # # Tắt Arrow nếu cần
        # spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

        # # Chuyển đổi Pandas DataFrame sang Spark DataFrame
        # data = features.to_dict(orient='records') 

        # spark_df_with_predictions = spark.createDataFrame(features)
        #connect and save data to hbase
        # connect_and_save_to_hbase(df_with_predictions)

        # insert_data_to_hbase(connection, df_with_predictions)
        connect_and_save_to_hbase(df_with_predictions.toPandas())
        # write_to_hbase(df_with_predictions, table_name="hbase-clv")
        # print("Sucess insert into hbase") 

    except Exception as e:
        logger.error(f"Error during model prediction: {e}")

def connect_and_save_to_hbase(df_with_predictions):
    print("Starting to consume...") 
    try:
        # Lấy kết nối từ pool
        with connection_pool.connection() as connection:
            insert_data_to_hbase(connection, df_with_predictions)
        print("Success insert into HBase")
    except Exception as e:
        print(f"Error during HBase operation: {e}")

def consume_and_preprocess(spark, predict_udf_func):
    try:
        kafka_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
        parsed_df = kafka_df.withColumn("data", from_json(col("value"), schema)).select("data.*")
        parsed_df = parsed_df.dropna()
      
        query = parsed_df \
            .writeStream \
            .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, predict_udf_func)) \
            .start()
        

        query.awaitTermination()
    except Exception as e:
        logger.exception(f"Error in consume_and_preprocess: {e}")