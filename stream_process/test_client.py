import threading
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType
import json

# Hàm gửi dữ liệu vào Kafka
def send_to_kafka(invoice_data, kafka_broker, topic):
    # Cấu hình Kafka Producer
    conf = {
        'bootstrap.servers': kafka_broker,
        'client.id': 'invoice-producer'
    }
    producer = Producer(conf)

    # Hàm báo cáo kết quả gửi dữ liệu
    def delivery_report(err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    # Gửi dữ liệu vào Kafka topic
    for record in invoice_data:
        producer.produce(topic, value=json.dumps(record), callback=delivery_report)

    # Đảm bảo tất cả thông điệp đã được gửi trước khi kết thúc
    producer.flush()

# Hàm xử lý dữ liệu từ Kafka và chuẩn hóa thành DataFrame
def process_from_kafka(kafka_broker, topic):
    # Khởi tạo Spark session
    spark = SparkSession.builder \
        .appName("InvoiceProcessing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest")  \
    .load()

    # Dữ liệu trong Kafka là dạng byte, cần chuyển về dạng chuỗi JSON
    invoice_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_data")

    # Định nghĩa schema để phân tích chuỗi JSON
    item_schema = StructType([
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DoubleType(), True)
    ])
    invoice_schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Items", ArrayType(item_schema), True)
    ])

    # Chuyển đổi chuỗi JSON thành DataFrame bằng `from_json`
    invoice_json_df = invoice_df.select(
        from_json(col("json_data"), invoice_schema).alias("data")
    ).select("data.*")

    # Tách các phần tử trong trường "Items" thành các dòng riêng biệt
    df_exploded = invoice_json_df.withColumn("Item", explode(col("Items"))).select(
        "InvoiceNo",
        "InvoiceDate",
        "CustomerID",
        "Country",
        "Item.StockCode",
        "Item.Description",
        "Item.Quantity",
        "Item.UnitPrice"
    )

    # Cấu hình việc lưu kết quả vào hệ thống đích (như Console hoặc một Kafka Topic khác)
    query = df_exploded.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Chạy Streaming
    query.awaitTermination()

# Hàm chính để điều phối các công việc
def main():
    # Dữ liệu hóa đơn mẫu
    # invoice_data = [
    #     {
    #         "InvoiceNo": "A12345",
    #         "InvoiceDate": "2024-12-10T15:30:00",
    #         "CustomerID": "C1001",
    #         "Country": "Vietnam",
    #         "Items": [
    #             {
    #                 "StockCode": "B67890",
    #                 "Description": "Product Name 1",
    #                 "Quantity": 2,
    #                 "UnitPrice": 50.0
    #             },
    #             {
    #                 "StockCode": "B12345",
    #                 "Description": "Product Name 2",
    #                 "Quantity": 1,
    #                 "UnitPrice": 30.0
    #             }
    #         ]
    #     }
    # ]

    # Thay đổi địa chỉ Kafka và topic theo yêu cầu của bạn
    kafka_broker = 'localhost:9093'
    topic = 'test'

    # # Tạo một thread để gửi dữ liệu vào Kafka
    # send_thread = threading.Thread(target=send_to_kafka, args=(invoice_data, kafka_broker, topic))

    # Tạo một thread để xử lý dữ liệu từ Kafka
    process_thread = threading.Thread(target=process_from_kafka, args=(kafka_broker, topic))

    # Bắt đầu cả hai thread
    # send_thread.start()
    process_thread.start()

    # Chờ các thread hoàn thành
    # send_thread.join()
    process_thread.join()

# Điểm bắt đầu chương trình
if __name__ == "__main__":
    main()
