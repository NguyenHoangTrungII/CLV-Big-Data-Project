import time
import threading
import logging
from stream_process.kafka.kafka_scripts.send_fake_data import send_data
from batch_process.spark.spark_scripts.stream_clv_prediction import consume_and_preprocess, schema, register_udf, model # Import the necessary functions and schema
from pyspark.sql import SparkSession

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global flag for stopping threads
stop_threads = False

def producer_thread():
    global stop_threads
    while not stop_threads:
        try:
            send_data()
            logger.info("Message sent to Kafka topic")
            time.sleep(5)
        except Exception as e:
            logger.exception(f"Error in producer_thread: {e}")


def consumer_thread(spark):
    global stop_threads
    predict_udf_func = register_udf(spark) #register here after spark is initialised.
    while not stop_threads:
        try:
            consume_and_preprocess(spark, predict_udf_func)
            time.sleep(3)
        except Exception as e:
            logger.exception(f"Error in consumer_thread: {e}")


if __name__ == "__main__":
    # Create SparkSession ONCE before threads start
    spark = SparkSession.builder \
        .appName("CLV Prediction") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    producer = threading.Thread(target=producer_thread)
    consumer = threading.Thread(target=consumer_thread, args=(spark,))

    producer.start()
    consumer.start()

    try:
        producer.join()
        consumer.join()
    except KeyboardInterrupt:
        stop_threads = True
        producer.join()
        consumer.join()
        logger.info("Graceful shutdown completed.")
        spark.stop()