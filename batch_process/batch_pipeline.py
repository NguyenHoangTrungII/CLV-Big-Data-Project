import time
import threading
from batch_process.hadoop.hadoop_scripts.hadoop_consumer import consume_hdfs
from batch_process.spark.spark_scripts.spark_processing import spark_processing
from stream_process.kafka.kafka_scripts.send_fake_data import send_data

# ================================
# Producer Function
# ================================
def producer_task():
    """
    Continuously sends fake data to Kafka at regular intervals.
    """
    while True:
        try:
            send_data()
            print("[Producer] Message sent to Kafka topic")
            
            # Sleep before sending the next set of data
            time.sleep(5)
        except Exception as e:
            print(f"[Producer] Error: {str(e)}")

# ================================
# Consumer Function
# ================================
def consumer_task():
    """
    Continuously consumes data from HDFS and processes it using Spark.
    """
    print("[Consumer] Starting to consume from Kafka...")
    while True:
        try:
            print("[Consumer] Consuming data from HDFS...")
            consume_hdfs()
            
            # Sleep before consuming the next batch
            time.sleep(3)
        except Exception as e:
            print(f"[Consumer] Error: {str(e)}")

# ================================
# Main: Thread Management
# ================================
if __name__ == "__main__":
    # Create threads for producer and consumer tasks
    producer_thread = threading.Thread(target=producer_task, name="ProducerThread")
    consumer_thread = threading.Thread(target=consumer_task, name="ConsumerThread")
    
    # Start threads
    producer_thread.start()
    consumer_thread.start()
    
    # Wait for threads to complete (they run indefinitely)
    producer_thread.join()
    consumer_thread.join()
