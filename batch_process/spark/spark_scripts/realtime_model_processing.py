import pandas as pd
import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, dayofweek, when

# Kafka configuration
KAFKA_TOPIC = 'CLV_system_nhtrung'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Spark session
# spark = SparkSession.builder \
#     .appName("RealTimeCLVModelProcessing") \
#     .config("spark.ui.port", "4042") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("CLV_Prediction") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()


# Kafka Consumer Setup
consumer = KafkaConsumer(KAFKA_TOPIC, 
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                             
                        )

# Define schema for incoming Kafka data (assuming JSON format)
# Here we do not need the schema for Spark DataFrame, just for structured reference.
def preprocess_data(message):
    # Convert message to DataFrame
    df = pd.DataFrame([message])
    
    # Data preprocessing steps:
    
    # Convert 'InvoiceDate' to datetime format
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')

    # Extract date and time-related features
    df['date'] = df['InvoiceDate'].dt.date
    df['hour'] = df['InvoiceDate'].dt.hour
    df['dayofweek'] = df['InvoiceDate'].dt.dayofweek
    df['weekend'] = df['dayofweek'].apply(lambda x: 1 if x >= 5 else 0)
    
    # Calculate Revenue
    df['Revenue'] = df['Quantity'] * df['UnitPrice']
    
    # Remove rows with invalid or missing CustomerID
    df_cleaned = df.dropna(subset=['CustomerID'])
    df_cleaned['CustomerID'] = df_cleaned['CustomerID'].astype(str)

    # Handle other columns with potential missing values
    df_cleaned['Description'] = df_cleaned['Description'].fillna('')
    df_cleaned['StockCode'] = df_cleaned['StockCode'].fillna('')
    df_cleaned['InvoiceNo'] = df_cleaned['InvoiceNo'].fillna(0)
    
    # Return the processed data
    return df_cleaned

# Kafka Consumer Loop for continuous data consumption
def consume_and_preprocess():
    for message in consumer:
        if message.value is None:
            print("Received null message, skipping processing.")
            continue  # Skip this message if it's null

        processed_data = preprocess_data(message.value)  # Preprocess each message and return the processed DataFrame
        print(f"Processed Data:\n{processed_data.head()}")  # Show the top rows of the processed data
        return processed_data

if __name__ == "__main__":
    consume_and_preprocess()
