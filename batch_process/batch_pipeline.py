# import time
# from batch_process.hadoop.hadoop_scripts.hadoop_consumer import consume_hdfs
# import threading
# from stream_process.kafka.kafka_scripts.send_fake_data import send_data

# from batch_process.spark.spark_scripts.spark_processing  import spark_processing

# def producer_thread():
#     while True:
#         try:
#             send_data()
#             print("Message sent to Kafka topic")

#             # Sleep for 5 seconds before collecting and sending the next set of data
#             time.sleep(5)

#         except Exception as e:
#             print(f"Error in producer_thread: {str(e)}")

# def consumer_thread():
#     print("Starting to consume from Kafka...")  # Add this log
#     while True:
#         try:
#             print("Consuming data from HDFS...")
#             consume_hdfs()
#             spark_processing()
#             # Sleep for a short interval before consuming the next message
#             time.sleep(3)
#         except Exception as e:
#             print(f"Error in consumer_thread: {str(e)}")

# # Create separate threads for producer and consumer
# producer_thread = threading.Thread(target=producer_thread)
# consumer_thread = threading.Thread(target=consumer_thread)

# # Start the threads
# producer_thread.start()
# consumer_thread.start()

# # Wait for the threads to finish (which will never happen in this case as they run infinitely)
# producer_thread.join()
# consumer_thread.join()


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
