# import time
# import threading
# import logging
# from pyspark.sql import SparkSession
# from stream_process.kafka.kafka_scripts.send_fake_data import send_data
# from batch_process.hadoop.hadoop_scripts.hadoop_consumer import consume_hdfs
# from batch_process.spark.spark_scripts.spark_processing import spark_processing
# from batch_process.spark.spark_scripts.stream_clv_prediction import consume_and_preprocess, register_udf

# # Initialize logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Global flag to stop threads gracefully
# stop_threads = False

# # ================================
# # Producer for Kafka
# # ================================
# def kafka_producer_thread():
#     """
#     Continuously sends fake data to Kafka.
#     """
#     global stop_threads
#     while not stop_threads:
#         try:
#             send_data()
#             logger.info("[Kafka Producer] Message sent to Kafka topic")
#             time.sleep(5)
#         except Exception as e:
#             logger.exception(f"[Kafka Producer] Error: {e}")

# # ================================
# # Batch Consumer (HDFS -> Spark)
# # ================================
# def batch_consumer_thread():
#     """
#     Consumes data from HDFS and processes it using Spark (batch).
#     """
#     global stop_threads
#     while not stop_threads:
#         try:
#             logger.info("[Batch Consumer] Consuming data from HDFS...")
#             consume_hdfs()
#             spark_processing(spark)
#             time.sleep(10)
#         except Exception as e:
#             logger.exception(f"[Batch Consumer] Error: {e}")

# # ================================
# # Stream Consumer (Kafka -> Spark)
# # ================================
# def stream_consumer_thread(spark):
#     """
#     Consumes data from Kafka and processes it using Spark Streaming.
#     """
#     global stop_threads
#     predict_udf_func = register_udf(spark)  # Register UDF after Spark initialization
#     while not stop_threads:
#         try:
#             logger.info("[Stream Consumer] Processing streaming data...")
#             consume_and_preprocess(spark, predict_udf_func)
#             time.sleep(3)
#         except Exception as e:
#             logger.exception(f"[Stream Consumer] Error: {e}")

# # ================================
# # Main: Thread Management
# # ================================
# if __name__ == "__main__":
#     # Initialize SparkSession
#     # spark = SparkSession.builder \
#     #     .appName("Batch and Stream Pipeline") \
#     #     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#     #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
#     #     .getOrCreate()

#     spark = SparkSession.builder \
#     .appName("Batch and Stream Pipeline") \
#     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#     .config("spark.executor.memory", "1g") \
#     .config("spark.executor.cores", "1") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
#     .getOrCreate()

#     # Create threads
#     kafka_producer = threading.Thread(target=kafka_producer_thread, name="KafkaProducerThread")
#     # batch_consumer = threading.Thread(target=batch_consumer_thread,  name="BatchConsumerThread")
#     stream_consumer = threading.Thread(target=stream_consumer_thread, args=(spark,), name="StreamConsumerThread")

#     # Start threads
#     kafka_producer.start()
#     # batch_consumer.start()
#     stream_consumer.start()

#     try:
#         # Wait for threads (infinite loop unless interrupted)
#         kafka_producer.join()
#         # batch_consumer.join()
#         stream_consumer.join()
#     except KeyboardInterrupt:
#         # Gracefully stop all threads
#         stop_threads = True
#         kafka_producer.join()
#         # batch_consumer.join()
#         stream_consumer.join()
#         logger.info("Pipeline shutdown completed.")
#         spark.stop()

import time
import happybase

def connect_to_hbase():
    try:
        connection = happybase.Connection(host='localhost')
        connection.open()
        print("Connected to HBase successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to HBase: {e}")
        return None

def insert_data(connection):
    try:
        table = connection.table('clv_predictions')
        row_key = '536365-17850'
        # data = {
        #     'cf:Quantity': b'6',
        #     'cf:UnitPrice': b'3.390000104904175',
        #     'cf:InvoiceDate': b'2010-12-01T08:26:00',
        #     'cf:dayofweek': b'4',
        #     'cf:weekend': b'0',
        #     'cf:Revenue': b'20.34000015258789',
        #     'cf:CLV_Prediction': b'31.9216251373291'
        # }
        data = {'cf:Quantity': b'1', 'cf:UnitPrice': b'1.0'}

        table.put(row_key, data)
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")

# Connect and insert
connection = connect_to_hbase()
if connection:
    insert_data(connection)
    connection.close()
