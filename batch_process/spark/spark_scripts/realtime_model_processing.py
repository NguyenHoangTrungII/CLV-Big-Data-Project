import time
import pandas as pd
from tensorflow import keras
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, from_json, to_timestamp, hour, dayofweek, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Kafka configuration
KAFKA_TOPIC = 'CLV_system_nhtrung'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

spark = SparkSession.builder \
   .appName("CLV Prediction") \
   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
   .getOrCreate()

model = None  

def load_model_once():
    global model
    if model is None:
        try:
            model = keras.models.load_model('/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras', compile=False)
            print("Model loaded successfully hihi.")
        except Exception as e:
            print(f"Error loading model: {e}")
            exit()
    return model

def preprocess_data(df):
    df = df.withColumn("InvoiceDate", regexp_replace(col("InvoiceDate"), "[^0-9\-: ]", ""))

    # Chuyển InvoiceDate sang timestamp nếu chưa
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))
    # Tính toán các cột bổ sung
    df = df \
        .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
        .withColumn("UnitPrice", col("UnitPrice").cast(FloatType())) \
        .withColumn("hour", hour(col("InvoiceDate"))) \
        .withColumn("dayofweek", dayofweek(col("InvoiceDate"))) \
        .withColumn("weekend", when(dayofweek(col("InvoiceDate")).isin(6, 7), 1).otherwise(0)) \
        .withColumn("Revenue", col("Quantity") * col("UnitPrice"))

    return df

def predict_clv(processed_data, model):
   
   model = load_model_once()  # Gọi hàm load mô hình khi cần

   # Ensure the dataframe has the necessary features for prediction
   features = processed_data[['Quantity', 'UnitPrice','hour','hour','hour','hour','hour', 'hour', 'dayofweek', 'weekend', 'Revenue']]


   # Convert to numpy array if needed
   features = features.values
  
   # Make predictions using the model
   prediction = model.predict(features)
  
   # Add the CLV predictions to the dataframe
   processed_data['CLV_Prediction'] = prediction
  
   return processed_data


def consume_and_preprocess():
    """
    Đọc dữ liệu từ Kafka, tiền xử lý và xuất ra console.
    """
    # Đọc dữ liệu từ Kafka
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Chuyển dữ liệu Kafka từ binary sang string
    df = df_raw.selectExpr("CAST(value AS STRING)")

    # Định nghĩa schema cho dữ liệu JSON
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

    df_parsed = df.withColumn("data", from_json(col("value"), schema)).select("data.*")


    # Chuyển kiểu dữ liệu cho các cột
    df_casted = df_parsed \
        .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
        .withColumn("UnitPrice", col("UnitPrice").cast(FloatType())) \
        .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))


    # Sử dụng mapGroupsWithState để xử lý và trả về từng dòng dữ liệu
    def process_with_state(df):
        try:
            model = keras.models.load_model('/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras', compile=False)
            print("Model loaded successfully kaka.")
        except Exception as e:
            print(f"Error loading model: {e}")
            exit()
        
        df_with_predictions = predict_clv(df, model)
        print(df_with_predictions[['InvoiceNo', 'CLV_Prediction']])



    # Ghi dữ liệu vào console (có thể thay bằng file hoặc hệ thống khác)
    query = df_parsed \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .foreachBatch(lambda df, epoch_id: process_with_state(df)) \
        .start()

    # Chờ đợi và giữ cho job chạy
    query.awaitTermination()

    return query


def consume_and_preprocess_with_spark(spark):
    """
    Đọc dữ liệu từ Kafka, tiền xử lý và xuất ra console.
    """
    # Đọc dữ liệu từ Kafka
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Chuyển dữ liệu Kafka từ binary sang string
    df = df_raw.selectExpr("CAST(value AS STRING)")

    # Định nghĩa schema cho dữ liệu JSON
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

    df_parsed = df.withColumn("data", from_json(col("value"), schema)).select("data.*")


    # Chuyển kiểu dữ liệu cho các cột
    df_casted = df_parsed \
        .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
        .withColumn("UnitPrice", col("UnitPrice").cast(FloatType())) \
        .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))


    # Sử dụng mapGroupsWithState để xử lý và trả về từng dòng dữ liệu
    def process_with_state(df):
        try:
            model = keras.models.load_model('/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras', compile=False)
            print("Model loaded successfully hoho.")
        except Exception as e:
            print(f"Error loading model: {e}")
            exit()
        
        df_with_predictions = predict_clv(df, model)
        print(df_with_predictions[['InvoiceNo', 'CLV_Prediction']])



    # Ghi dữ liệu vào console (có thể thay bằng file hoặc hệ thống khác)
    query = df_parsed \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .foreachBatch(lambda df, epoch_id: process_with_state(df)) \
        .start()

    # Chờ đợi và giữ cho job chạy
    query.awaitTermination()

    return query

def write_to_console(df):
   """
   Ghi dữ liệu đã xử lý ra console (có thể thay đổi thành các sink khác).
   """
   query = df \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", False) \
       .start()


   # Đợi cho đến khi quá trình streaming kết thúc
   query.awaitTermination()


if __name__ == "__main__":
   # Bắt đầu quá trình lấy và xử lý dữ liệu từ Kafka
   processed_data = consume_and_preprocess()
 
   # Ghi kết quả ra console
   write_to_console(processed_data)
