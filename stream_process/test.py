from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.sql.functions import explode

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def test_hdfs_connection():
    # Địa chỉ IP của namenode
    namenode_ip = "namenode"  # Thay bằng tên container hoặc địa chỉ IP của Namenode
    hdfs_url = f"hdfs://{namenode_ip}:9000"  # Cập nhật URL HDFS

    # Tạo một SparkSession
    spark = SparkSession.builder \
        .appName("CLV Prediction") \
        .master("spark://172.31.56.16:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        \
            .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("hbase.zookeeper.quorum", "localhost:2181") \
        .config("zookeeper.znode.parent", "/hbase") \
        .config("spark.driver.host", "172.31.56.16")\
        .config("spark.executor.heartbeatInterval", "60s")  \
        .config("spark.network.timeout", "120s")  \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.cores.max", "4")  \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()


    # Định nghĩa schema tường minh cho dữ liệu
    schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Items", ArrayType(StructType([
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", FloatType(), True),
            StructField("UnitPrice", FloatType(), True)
        ])), True)
    ])

    # Đọc dữ liệu từ HDFS với schema tường minh
    input_path = f"hdfs://{namenode_ip}:9000/batch-layer/raw_data.json"  # Cập nhật đường dẫn

    try:
        print("Đang đọc dữ liệu từ HDFS...")
        # Đọc dữ liệu từ tệp JSON với schema tường minh
        df = spark.read.json(input_path)

        # Hiển thị cấu trúc dữ liệu
        print("Cấu trúc dữ liệu:")
        df.printSchema()

        # Explode the `Items` array into rows
        exploded_df = df.withColumn("Items", df["Items"]).selectExpr(
            "InvoiceNo", "InvoiceDate", "CustomerID", "Country", "explode(Items) as Items"
        )
        
        # Extract fields from `Items`
        # transformed_rdd = exploded_df.select(
        #     "InvoiceNo",
        #     "InvoiceDate",
        #     "CustomerID",
        #     "Country",
        #     "Items.StockCode",
        #     "Items.Description",
        #     "Items.Quantity",
        #     "Items.UnitPrice"
        # )

        # # Convert to a Python list of dictionaries for further processing
        # transformed_rdd = transformed_rdd.rdd.map(lambda row: {
        #     'InvoiceNo': row.InvoiceNo,
        #     'StockCode': row.StockCode,
        #     'Description': row.Description,
        #     'Quantity': row.Quantity,
        #     'InvoiceDate': row.InvoiceDate,
        #     'UnitPrice': row.UnitPrice,
        #     'CustomerID': row.CustomerID,
        #     'Country': row.Country
        # })

        # transformed_data = transformed_rdd.collect()

        # print("data from batch_layer", transformed_data )

        # # Sử dụng explode để tách các sản phẩm trong Items thành các dòng riêng biệt
        # # df_exploded = df.withColumn("Item", explode(df.Items))

        # # Hiển thị dữ liệu đã được nở
        # # df_exploded.select("InvoiceNo", "InvoiceDate", "CustomerID", "Country", 
        # #                    "Item.StockCode", "Item.Description", "Item.Quantity", "Item.UnitPrice").show(5)
        # transformed_data.show(5, truncate=False)
    except Exception as e:
        print(f"Đã xảy ra lỗi khi đọc dữ liệu từ HDFS: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_hdfs_connection()
