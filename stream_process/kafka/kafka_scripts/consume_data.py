from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
from tensorflow import keras

# Load the pre-trained CLV model
try:
    model = keras.models.load_model('/home/nhtrung/product_recommendation_system/stream_process/model/CLV_model.pkl')    
    print("Model loaded successfully.")
except Exception as e:
    print(f"Error loading model: {e}")

# Function to preprocess data
def preprocess_data(data):
    data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])
    data['InvoiceDate'] = data['InvoiceDate'].astype('int64') // 10**9  # Convert to Unix timestamp
    data['TotalValue'] = data['Quantity'] * data['UnitPrice']
    
    # Additional feature engineering if necessary
    # Handle outliers or transformations consistent with training data
    return data

# Function to create time-step sequences for LSTM
def create_dataset(data, time_step=5):
    X, y = [], []
    for i in range(len(data) - time_step):
        X.append(data[i:i + time_step, 0])
        y.append(data[i + time_step, 0])
    return np.array(X), np.array(y)

# Convert data for LSTM format
def format_for_prediction(data, time_step=5):
    data = data['TotalValue'].values.reshape(-1, 1)  # Adjust to expected format
    X, _ = create_dataset(data, time_step)
    return X.reshape(X.shape[0], X.shape[1], 1)

# Kafka consumer setup
consumer = KafkaConsumer(
    'CLV_system_nhtrung',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def consume():
    # Process messages from Kafka
    for message in consumer:
        try:
            # Convert message to DataFrame
            raw_data = pd.DataFrame([message.value])
            
            # Preprocess data
            data = preprocess_data(raw_data)
            
            # Format data for prediction
            X = format_for_prediction(data)
            
            # Make prediction
            prediction = model.predict(X)
            
            # Log or output the prediction
            print(f"Prediction for data {data['CustomerID'].values[0]}: {prediction[-1][0]}")
            
        except Exception as e:
            print(f"Error processing message {message}: {e}")

