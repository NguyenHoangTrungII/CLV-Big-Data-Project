import time

from stream_process.kafka.kafka_scripts.consume_data import consume_and_predict
import threading
from stream_process.kafka.kafka_scripts.send_fake_data import send_data


def producer_thread():
    while True:
        try:
            send_data()
            print("Message sent to Kafka topic")

            # Sleep for 5 seconds before collecting and sending the next set of data
            time.sleep(5)

        except Exception as e:
            print(f"Error in producer_thread: {str(e)}")


def consumer_thread():
    """Thread function to consume data from Kafka and perform ML inference."""
    while True:
        try:
            # Call the consumer function to consume and process messages
            # consume()
            consume_and_predict()

            # print(df_with_predictions[['InvoiceNo', 'CLV_Prediction']])  # Example: Print InvoiceNo and predicted CLV


            # Optional: Short pause before consuming the next message
            time.sleep(3)
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")

# Create and start threads for both the producer and consumer
producer = threading.Thread(target=producer_thread, name="ProducerThread")
consumer = threading.Thread(target=consumer_thread, name="ConsumerThread")

producer.start()
consumer.start()

# Keep the main program running to allow threads to continue indefinitely
producer.join()
consumer.join()
