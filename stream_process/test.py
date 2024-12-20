# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
# from pyspark.sql.functions import explode

# import os
# import sys
# import sys
# import logging


# # os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.10"
# # os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3.10"

# print("PYSPARK_PYTHON:", os.environ.get('PYSPARK_PYTHON'))
# print("PYSPARK_DRIVER_PYTHON:", os.environ.get('PYSPARK_DRIVER_PYTHON'))



# def test_hdfs_connection():
#     # Địa chỉ IP của namenode
#     namenode_ip = "namenode"  # Thay bằng tên container hoặc địa chỉ IP của Namenode
#     hdfs_url = f"hdfs://{namenode_ip}:9000"  # Cập nhật URL HDFS

#     # Tạo một SparkSession
#     spark = SparkSession.builder \
#         .appName("CLV Prediction") \
#         .master("spark://172.31.56.16:7077") \
#         .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#         .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#         \
#         .config("spark.jars.packages", 
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
#         .config("hbase.zookeeper.quorum", "localhost:2181") \
#         .config("zookeeper.znode.parent", "/hbase") \
#         .config("spark.driver.host", "172.31.56.16")\
#         .config("spark.executor.heartbeatInterval", "60s")  \
#         .config("spark.network.timeout", "120s")  \
#         .config("spark.executor.memory", "2g") \
#         .config("spark.driver.memory", "2g") \
#         .config("spark.cores.max", "4")  \
#         .config("spark.sql.shuffle.partitions", "100") \
#         .config("spark.pyspark.python", "/usr/local/bin/python3.10") \
#         .config("spark.pyspark.driver.python", "/usr/local/bin/python3.10") \
#         .getOrCreate()

#     # Định nghĩa schema tường minh cho dữ liệu
#     schema = StructType([
#         StructField("InvoiceNo", StringType(), True),
#         StructField("InvoiceDate", StringType(), True),
#         StructField("CustomerID", StringType(), True),
#         StructField("Country", StringType(), True),
#         StructField("Items", ArrayType(StructType([
#             StructField("StockCode", StringType(), True),
#             StructField("Description", StringType(), True),
#             StructField("Quantity", FloatType(), True),
#             StructField("UnitPrice", FloatType(), True)
#         ])), True)
#     ])

#     # Đọc dữ liệu từ HDFS với schema tường minh
#     input_path = f"hdfs://{namenode_ip}:9000/batch-layer/raw_data.json"  # Cập nhật đường dẫn

#     try:
#         print("Đang đọc dữ liệu từ HDFS...")
#         # Đọc dữ liệu từ tệp JSON với schema tường minh
#         df = spark.read.option("multiline", "true").schema(schema).json(input_path)

#         df.show()

#         # Hiển thị cấu trúc dữ liệu
#         print("Cấu trúc dữ liệu:")
#         df.printSchema()

#         # Explode the `Items` array into rows
#         exploded_df = df.withColumn("Items", df["Items"]).selectExpr(
#             "InvoiceNo", "InvoiceDate", "CustomerID", "Country", "explode(Items) as Items"
#         )
        
#         # Extract fields from `Items`
#         transformed_df = exploded_df.select(
#             "InvoiceNo",
#             "InvoiceDate",
#             "CustomerID",
#             "Country",
#             "Items.StockCode",
#             "Items.Description",
#             "Items.Quantity",
#             "Items.UnitPrice"
#         )
        

#         # # Convert to a Python list of dictionaries for further processing
#         transformed_rdd = transformed_df.rdd.map(lambda row: {
#             'InvoiceNo': row.InvoiceNo,
#             'StockCode': row.StockCode,
#             'Description': row.Description,
#             'Quantity': row.Quantity,
#             'InvoiceDate': row.InvoiceDate,
#             'UnitPrice': row.UnitPrice,
#             'CustomerID': row.CustomerID,
#             'Country': row.Country
#         })

#         transformed_data = transformed_rdd.collect()

#         print("data from batch_layer", transformed_data )


#         # Sử dụng explode để tách các sản phẩm trong Items thành các dòng riêng biệt
#         # df_exploded = df.withColumn("Item", explode(df.Items))

#         # Hiển thị dữ liệu đã được nở
#         # df_exploded.select("InvoiceNo", "InvoiceDate", "CustomerID", "Country", 
#         #                    "Item.StockCode", "Item.Description", "Item.Quantity", "Item.UnitPrice").show(5)
#         # transformed_data.show(5, truncate=False)
#     except Exception as e:
#         print(f"Đã xảy ra lỗi khi đọc dữ liệu từ HDFS: {e}")
#     finally:
#         spark.stop()

# if __name__ == "__main__":
#     test_hdfs_connection()
import os        
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def save_to_postgres(df, table_name, db_url, db_properties, mode="append"):
    """
    Save a Spark DataFrame to a PostgreSQL database.

    Parameters:
        df (DataFrame): The Spark DataFrame to save.
        table_name (str): The target table name in the PostgreSQL database.
        db_url (str): The JDBC URL for connecting to the PostgreSQL database.
        db_properties (dict): A dictionary containing PostgreSQL connection properties (user, password, driver).
        mode (str): The save mode, default is "append". Other options include "overwrite" and "ignore".

    Returns:
        None
    """
    try:
        df.write.jdbc(url=db_url, table=table_name, mode=mode, properties=db_properties)
        print(f"DataFrame has been successfully saved to table '{table_name}' in PostgreSQL.")
    except Exception as e:
        print(f"Error saving DataFrame to PostgreSQL: {e}")

# Example usage
if __name__ == "__main__":
    # Initialize SparkSession
    # spark = SparkSession.builder \
    #     .appName("Save to PostgreSQL") \
    #     .config("spark.jars", "/home/nhtrung/CLV-Big-Data-Project/batch_process/spark/jar/postgresql-42.7.4.jar") \
    #     .getOrCreate()

    # Đặt phiên bản Python cho PySpark
    os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.10"
    os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3.10"

    # Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("CLV Prediction") \
        .master("spark://172.31.56.16:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.jars", "/home/nhtrung/CLV-Big-Data-Project/batch_process/spark/jar/postgresql-42.7.4.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("hbase.zookeeper.quorum", "localhost:2181") \
        .config("zookeeper.znode.parent", "/hbase") \
        .config("spark.driver.host", "172.31.56.16") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.network.timeout", "120s") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.cores.max", "4") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()

    # Phiên Spark session đã sẵn sàng để sử dụng
    print("Spark session đã được tạo thành công!")


    # Example DataFrame


    data = [
        ('United Kingdom', 12345, 'Product A', '2024-12-20 10:00:00', 'INV001', 10, 'A001', 15.5, 155.0, 33.87),
        ('France', 67890, 'Product B', '2024-12-21 15:30:00', 'INV002', 5, 'B002', 25.0, 125.0, 50.22),
        ('Germany', 11223, 'Product C', '2024-12-22 09:15:00', 'INV003', 20, 'C003', 8.75, 175.0, 40.10),
        ('Italy', 33456, 'Product D', '2024-12-23 14:45:00', 'INV004', 8, 'D004', 22.0, 176.0, 55.00)
    ]

    
    columns = ["Country", "CustomerID", "Description", "InvoiceDate", "InvoiceNo", "Quantity", "StockCode", "UnitPrice", "Revenue", "CLV_Prediction"]
    df = spark.createDataFrame(data, columns).withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))
    # PostgreSQL connection properties
    db_properties = {
        "user": "postgres",
        "password": "123",
        "driver": "org.postgresql.Driver"
    }
    db_url = "jdbc:postgresql://172.20.0.8:5432/airflow"

    # Save DataFrame to PostgreSQL
    save_to_postgres(df, "sales_data", db_url, db_properties)
