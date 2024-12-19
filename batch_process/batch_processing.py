# batch_layer.py
import sys

sys.path.append('/usr/local/airflow')


from batch_process.spark.spark_scripts.spark_processing import create_spark_session, spark_processing, spark_processing_v2

def batch_layer():
    try:
        print("Starting Spark processing version 2...")

        # Khởi tạo SparkSession
        spark = create_spark_session()

        # Truyền SparkSession vào spark_processing_v2
        spark_processing_v2(spark)

        print("Spark processing completed.")
    except Exception as e:
        print(f"Error in batch_layer: {str(e)}")