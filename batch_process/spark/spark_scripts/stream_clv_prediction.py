import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, from_json, to_timestamp, hour, dayofweek, when, pandas_udf, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from tensorflow import keras
import numpy as np
import pandas as pd
import threading
from stream_process.hbase.hbase_scripts.hbase_consumer import insert_data_to_hbase, connect_to_hbase

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_TOPIC = 'CLV_system_nhtrung'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

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

# Load the model ONCE (outside any threads)
model = None
try:
    model = keras.models.load_model('/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras', compile=False)
    logger.info("Model loaded successfully.")
except Exception as e:
    logger.error(f"Error loading model: {e}")
    exit(1)

# Preprocessing Function (optimized)
# def preprocess_data(df):
#     df = df.withColumn("InvoiceDate", regexp_replace(col("InvoiceDate"), "[^0-9\-: ]", ""))
#     df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))
#     df = df.select("InvoiceNo", "CustomerID", "Quantity", "UnitPrice", "InvoiceDate")  #Only select necessary columns.
#     return df

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
    df = df.select("InvoiceNo", "CustomerID", "Quantity", "UnitPrice", "InvoiceDate", "Revenue", "hour","hour", "hour", "dayofweek", "weekend")
    pandas_df = df.toPandas()
    return pandas_df

def register_udf(spark):
    @pandas_udf("double")
    def predict_udf(input_data_values):

        global model

        try:
            predictions = model.predict(input_data_values)
        except Exception as e:
            print(f"Error in predict {e}")

        return predictions
        # try:
        #     global model

        #     predictions = model.predict(input_data_values)

            # input_data = pd.DataFrame({
            #     'InvoiceNo': invoice_no,
            #     'Quantity': quantity,
            #     'UnitPrice': unit_price,
            #     'InvoiceDate': invoice_date,
            #     'CustomerID': customer_id
            # })
            
            
            # Convert InvoiceDate to datetime with nanosecond precision
        #     input_data['InvoiceDate'] = pd.to_datetime(input_data['InvoiceDate'], errors='coerce', format='%Y-%m-%d %H:%M:%S')

        #     # Ensure the 'InvoiceDate' column is in datetime64[ns] format
        #     input_data['InvoiceDate'] = input_data['InvoiceDate'].astype('datetime64[ns]')

        #     # Handle missing values or invalid data
        #     input_data = input_data.dropna(subset=['InvoiceDate', 'Quantity', 'UnitPrice'])
            


        #     input_data['Quantity'] = input_data['Quantity'].astype(np.float32)
        #     input_data['UnitPrice'] = input_data['UnitPrice'].astype(np.float32)
        #     input_data['Revenue'] = input_data['Quantity'] * input_data['UnitPrice']
        #     input_data['hour'] = input_data['InvoiceDate'].dt.hour
            
        #     input_data['hour1'] = input_data['InvoiceDate'].dt.hour
        #     input_data['hour2'] = input_data['InvoiceDate'].dt.hour
        #     input_data['hour3'] = input_data['InvoiceDate'].dt.hour
        #     input_data['hour4'] = input_data['InvoiceDate'].dt.hour


        #     input_data['dayofweek'] = input_data['InvoiceDate'].dt.dayofweek
        #     input_data['weekend'] = input_data['dayofweek'].apply(lambda x: 1 if x >= 5 else 0)
        #     input_data = input_data.replace([np.inf, -np.inf], np.nan).dropna()
        #     # input_data_values = np.array(input_data[['Quantity', 'UnitPrice', 'hour', 'dayofweek', 'weekend', 'Revenue']].values, dtype=np.float32)

        #     input_data_values = np.array(input_data.values, dtype=np.float32)
        #     #Check that the shape is correct
        #     logger.info(f"Shape of input data for prediction: {input_data_values.shape}")
        #     if input_data_values.shape[0] == 0:
        #         logger.warning("Empty input data for prediction. Returning default value.")
        #         return pd.Series([0.0] * len(invoice_no))

        #     predictions = model.predict(input_data_values)
        #     return pd.Series(predictions.flatten())
        # except Exception as e:
        #     logger.exception(f"Error during model prediction: {e}")
        #     #Return a default value if prediction fails.
        #     return pd.Series([0.0] * len(invoice_no))


        

        # print("Data predict", pd.Series(predictions.flatten()))
        
        return pd.Series(predictions.flatten())
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

# def process_batch(batch_df, batch_id, predict_udf):
#     logger.info(f"Processing batch {batch_id}")
#     processed_df = preprocess_data(batch_df)
#     try:
#         results_df = processed_df.withColumn(
#         "CLV_Prediction",
#         predict_udf(processed_df["InvoiceNo"], processed_df["Quantity"], processed_df["UnitPrice"], processed_df["InvoiceDate"], processed_df["CustomerID"])
#         )
#     except Exception as e:
#         logger.exception(f"Error in process_batch {e}")

#     results_df.show()  # Hoặc ghi vào hệ thống tệp nếu cần

#     connect_and_save_to_hbase(results_df.toPandas())
#     # results_df.show(10, truncate=False)

#     # print("Data in process batch: ", results_df.show(10, truncate=False) )


def process_batch(batch_df, batch_id, predict_udf):
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty!")
        return

    # Xử lý batch
    processed_df = preprocess_data(batch_df)

    # # Hành động Spark - ghi hoặc hiển thị
    # print(f"Processed Batch {batch_id}:")
    # processed_df.show(truncate=False)

    # # Nếu bạn muốn lưu dữ liệu, hãy sử dụng `.write`
    # processed_df.write.mode("append").format("console").save()
   

   # Thực hiện dự đoán với mô hình ML
    try:
        features = processed_df
        predictions = model.predict(features)
        processed_df["CLV_Prediction"] = predictions

        # Log kết quả
        print("Predicted Batch Data:")
        print(processed_df)
    except Exception as e:
        logger.error(f"Error during model prediction: {e}")
        
def consume_and_preprocess(spark, predict_udf_func):
    try:
        kafka_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")
        parsed_df = kafka_df.withColumn("data", from_json(col("value"), schema)).select("data.*")
        # parsed_df = parsed_df.dropna()

         
        query = parsed_df \
            .writeStream \
            .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, predict_udf_func)) \
            .start()
        

        query.awaitTermination()
    except Exception as e:
        logger.exception(f"Error in consume_and_preprocess: {e}")