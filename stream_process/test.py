# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, FloatType

# def create_spark_session():
#     """
#     Create Spark Session with HBase connector using Maven dependencies
#     """
#     spark = SparkSession.builder \
#         .appName("HBaseConnector") \
#         .config("spark.jars", "/home/nhtrung/shc/core/target/shc-core-1.1.3-2.4-s_2.11.jar") \
#         .getOrCreate()
#     return spark

# def write_to_hbase(spark, dataframe, table_name):
#     """
#     Write Spark DataFrame to HBase
#     """
#     try:
#         # HBase catalog configuration
#         hbase_catalog = """
#         {
#             "table": {
#                 "namespace": "default",
#                 "name": "hbase-clv"
#             },
#             "rowkey": "key",
#             "columns": {
#                 "key": {
#                     "cf": "cf",
#                     "col": "key",
#                     "type": "string"
#                 },
#                 "invoice_no": {
#                     "cf": "cf",
#                     "col": "invoice_no",
#                     "type": "string"
#                 },
#                 "clv_prediction": {
#                     "cf": "cf",
#                     "col": "clv_prediction",
#                     "type": "float"
#                 }
#             }
#         }
#         """


#         print("HBase Catalog:")
#         print(hbase_catalog)

#         # Write data to HBase
#         dataframe.write \
#             .format("org.apache.hadoop.hbase.spark") \
#             .option("hbase.catalog", hbase_catalog) \
#             .mode("append") \
#             .save()
        
#         print(f"Successfully wrote data to HBase table: {table_name}")
    
#     except Exception as e:
#         print(f"Error writing to HBase: {e}")
#         import traceback
#         traceback.print_exc()

# def main():
#     # Step 1: Create Spark session
#     spark = create_spark_session()
    
#     # Step 2: Define schema
#     schema = StructType([
#         StructField("key", StringType(), False),
#         StructField("invoice_no", StringType(), True),
#         StructField("clv_prediction", FloatType(), True)
#     ])
    
#     # Step 3: Create sample data
#     data = [
#         ("row1", "INV001", 100.5),
#         ("row2", "INV002", 200.7),
#         ("row3", "INV003", 150.3)
#     ]
    
#     # Step 4: Create DataFrame
#     df = spark.createDataFrame(data, schema)
    
#     # Step 5: Write DataFrame to HBase
#     write_to_hbase(spark, df, "hbase-clv")

# if __name__ == "__main__":
#     main()


from hdfs import InsecureClient

hdfs_host = 'localhost'
hdfs_port = 50070
client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user='hadoop')

try:
    status = client.status('/')
    print("HDFS Root Directory Status:", status)
except Exception as e:
    print("Error connecting to HDFS:", e)
