# batch_layer.py
import sys

sys.path.append('/usr/local/airflow')

from pyspark.sql import SparkSession
from pyspark.sql import Row

from batch_process.spark.spark_scripts.spark_processing import create_spark_session, create_spark_session_v2, spark_processing, spark_processing_v2

def batch_layer():
    try:
        print("Starting Spark processing version 2...")

        # Khởi tạo SparkSession
        try:
            spark = create_spark_session_v2()
            print("Sucess create spark." , spark.version)
        except Exception as e:
            print(f"Error in create spark: {str(e)}" )

        # Truyền SparkSession vào spark_processing_v2
        spark_processing_v2(spark)

        print("Spark processing completed.")
    except Exception as e:
        print(f"Error in batch_layer: {str(e)}")


def batch_layer_v2():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Simple Spark Job") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Dummy data
    data = [
        Row(id=1, name="Alice", age=25),
        Row(id=2, name="Bob", age=30),
        Row(id=3, name="Charlie", age=35)
    ]

    # Create DataFrame
    df = spark.createDataFrame(data)

    # Show the DataFrame
    df.show()

    # Save the result to CSV
    # df.write.csv("/tmp/output.csv", header=True)