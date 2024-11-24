# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import logging
# from stream_process.kafka.kafka_scripts.send_fake_data import send_data
# from batch_process.hadoop.hadoop_scripts.hadoop_consumer import consume_hdfs
# from batch_process.spark.spark_scripts.spark_processing import spark_processing
# from batch_process.spark.spark_scripts.stream_clv_prediction import consume_and_preprocess, register_udf
# from pyspark.sql import SparkSession

# # Initialize logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Global flag for stopping threads gracefully
# stop_threads = False

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("Batch and Stream Pipeline") \
#     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
#     .getOrCreate()

# # Define HDFS path
# hdfs_path = "/path/to/your/hdfs/data"

# # Producer task (Kafka)
# def kafka_producer_task():
#     try:
#         send_data()
#         logger.info("[Kafka Producer] Message sent to Kafka topic")
#     except Exception as e:
#         logger.exception(f"[Kafka Producer] Error: {e}")

# # Batch consumer task (HDFS -> Spark)
# def batch_consumer_task():
#     try:
#         consume_hdfs(hdfs_path, spark)
#         spark_processing(spark)
#     except Exception as e:
#         logger.exception(f"[Batch Consumer] Error: {e}")

# # Stream consumer task (Kafka -> Spark)
# def stream_consumer_task():
#     try:
#         predict_udf_func = register_udf(spark)
#         consume_and_preprocess(spark, predict_udf_func)
#     except Exception as e:
#         logger.exception(f"[Stream Consumer] Error: {e}")

# # Define Airflow DAG
# dag = DAG(
#     'batch_and_stream_pipeline',
#     default_args={
#         'owner': 'airflow',
#         'retries': 3,
#         'retry_delay': timedelta(minutes=5),
#     },
#     description='Pipeline for Batch and Stream processing with Spark and Kafka',
#     schedule_interval='@daily',  # or any custom schedule
#     start_date=datetime(2024, 11, 22),
#     catchup=False,
# )

# # Create tasks for each operation
# producer_task = PythonOperator(
#     task_id='kafka_producer',
#     python_callable=kafka_producer_task,
#     dag=dag,
# )

# batch_consumer_task = PythonOperator(
#     task_id='batch_consumer',
#     python_callable=batch_consumer_task,
#     dag=dag,
# )

# stream_consumer_task = PythonOperator(
#     task_id='stream_consumer',
#     python_callable=stream_consumer_task,
#     dag=dag,
# )

# # Set task dependencies
# producer_task >> batch_consumer_task >> stream_consumer_task