# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()
# data_path = "hdfs://localhost:9000/user/hadoop/product_data/"

# def process_data():
#     df = spark.read.json(data_path)
#     processed_df = df.groupBy("product_id").count()  # Ví dụ: Đếm số lượng sản phẩm
#     processed_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/processed_data/")

# if __name__ == "__main__":
#     process_data()

from spark.spark_scripts.spark_processing import spark_processing

from postgres.save_preprocessed_data import  save_data


def batch_layer():
    data = spark_processing()
    save_data(data)
