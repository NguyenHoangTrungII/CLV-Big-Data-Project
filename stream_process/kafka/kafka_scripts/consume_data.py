import pandas as pd
from tensorflow import keras
import json
from kafka import KafkaConsumer
from batch_process.spark.spark_scripts.realtime_model_processing import preprocess_data

# Load the pre-trained model
try:
    model = keras.models.load_model('/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras', compile=False)
    print("Model loaded successfully.")
except Exception as e:
    print(f"Error loading model: {e}")
    exit()

# Kafka Consumer Setup
KAFKA_TOPIC = 'CLV_system_nhtrung'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
consumer = KafkaConsumer(KAFKA_TOPIC, 
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def predict_clv(processed_data):
    # Ensure the dataframe has the necessary features for prediction
    features = processed_data[['Quantity', 'UnitPrice','hour','hour','hour','hour','hour', 'hour', 'dayofweek', 'weekend', 'Revenue']]

    # Convert to numpy array if needed
    features = features.values
    
    # Make predictions using the model
    prediction = model.predict(features)
    
    # Add the CLV predictions to the dataframe
    processed_data['CLV_Prediction'] = prediction
    
    return processed_data

def consume_and_predict():
    print("Starting to consume and predict...")
    for message in consumer:
        if message.value is None:
            print("Received null message, skipping processing.")
            continue

        # Step 1: Preprocess the incoming message
        processed_data = preprocess_data(message.value)
        if processed_data is None or processed_data.empty:
            print("No data after preprocessing, skipping prediction.")
            continue

        print(f"Processing data for InvoiceNo: {processed_data['InvoiceNo'].iloc[0]}")

        # Step 2: Predict CLV for the preprocessed data
        df_with_predictions = predict_clv(processed_data)

        # Step 3: Print or store the predictions
        print(df_with_predictions[['InvoiceNo', 'CLV_Prediction']])

if __name__ == "__main__":
    consume_and_predict()
