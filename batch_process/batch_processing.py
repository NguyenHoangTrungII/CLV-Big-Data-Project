# batch_layer.py
import sys

sys.path.append('/usr/local/airflow')


from batch_process.spark.spark_scripts.spark_processing import spark_processing

def batch_layer():
    try:
        print("Starting Spark processing...")
        spark_processing()  # Fetch data using Spark
        print("Spark processing completed.")
    except Exception as e:
        print(f"Error in batch_layer: {str(e)}")
