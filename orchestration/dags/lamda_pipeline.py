import sys
import os
sys.path.append('/usr/local/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 22),
    'retries': 1,
}

# Define the function for batch processing
def batch_layer_v2():
    # Create SparkSession
    os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.10"
    os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3.10"

    spark = SparkSession.builder \
    .appName("CLV Prediction IN airflow") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.host", "172.19.0.9") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.cores.max", "4") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("hbase.zookeeper.quorum", "zookeeper:2181") \
    .config("zookeeper.znode.parent", "/hbase") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .getOrCreate()

# Kiểm tra xem SparkSession đã được tạo thành công chưa
    if spark.version:
        print("SparkSession đã được tạo thành công. Phiên bản Spark:", spark.version)
    else:
        print("Không thể tạo SparkSession.")

    # Dummy data
    data = [
        Row(id=1, name="Alice", age=25),
        Row(id=2, name="Bob", age=30),
        Row(id=3, name="Charlie", age=35)
    ]

    try:

        print(spark)

        # Create DataFrame
        df = spark.createDataFrame(data)

        # Show the DataFrame
        df.show()
    except Exception as e:
        print(f"Error in batch_layer: {str(e)}")



# Define the DAG
with DAG(
    'simple_spark_dag',
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual execution
) as dag:

    # Define the PythonOperator task
    batch_layer_task = PythonOperator(
        task_id="batchlayer",
        python_callable=batch_layer_v2  # Pass the function reference (without parentheses)
    )

    # Set task dependencies
    batch_layer_task
