import time
from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
from tensorflow import keras
import os
from keras.layers import LSTM, Dense
# Define custom_objects for standard layers just in case Keras is treating them as custom
custom_objects = {
    'LSTM': LSTM,
    'Dense': Dense
}
# Get the current directory
current_dir = os.path.dirname(os.path.abspath(__file__)) 

# Construct the path to the model file
model_path = os.path.join(current_dir, 'CLV.h5')

print("Model path", {model_path})
# Load the pre-trained CLV model
try:
    model = keras.models.load_model('/home/nhtrung/main/CLV-Big-Data-Project/stream_process/model/CLV.h5', 
                       custom_objects=custom_objects, 
                       compile=False)
    print("Model loaded successfully.")
    
except Exception as e:
    print(f"Error loading model: {e}")
    exit ()

# time_step = 5
# buffer_size = 10  # Adjust as needed based on your model's needs
# data_buffer = []

# # Function to preprocess data
# def preprocess_data(data):
#     data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])
#     data['InvoiceDate'] = data['InvoiceDate'].astype('int64') // 10**9
#     data['TotalValue'] = data['Quantity'] * data['UnitPrice']
#     # ... handle other preprocessing based on your model requirements ...
#     return data

# # Function to create time-step sequences for LSTM
# def create_dataset(data, time_step=5):
#     X, y = [], []
#     for i in range(len(data) - time_step):
#         X.append(data[i:i + time_step, 0])
#         y.append(data[i + time_step, 0])
#     return np.array(X), np.array(y)

# # Convert data for LSTM format
# # def format_for_prediction(data, time_step=5):
# #     data = data['TotalValue'].values.reshape(-1, 1)  # Adjust to expected format
# #     X, _ = create_dataset(data, time_step)
# #     return X.reshape(X.shape[0], X.shape[1], 1)

# def format_for_prediction(data, time_step=5):
#     # Get the 'TotalValue' column as a numpy array
#     total_value = data['TotalValue'].values

#     # Reshape into a 2D array with one column
#     total_value = total_value.reshape(-1, 1)

#     # Create time-step sequences
#     X, _ = create_dataset(total_value, time_step)

#     # Reshape for LSTM input
#     return X.reshape(X.shape[0], X.shape[1], 1)

# # Kafka consumer setup
# consumer = KafkaConsumer(
#     'CLV_system_nhtrung',
#     bootstrap_servers='localhost:9092',
#     auto_offset_reset='earliest',
#     value_deserializer=lambda v: json.loads(v.decode('utf-8'))
# )

# def consume():
#     # Process messages from Kafka
#     for message in consumer:
#         try:
#             # Convert message to DataFrame
#             raw_data = pd.DataFrame([message.value], columns=['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'])   ;
#             # Preprocess data
#             # data = preprocess_data(raw_data)
            
#             data_buffer.append(raw_data)

#             # # Format data for prediction
#             # X = format_for_prediction(data)
#             # print(f"Data fprmat predict {X}")
#             # time.sleep(3)

#             # # Make prediction
#             # prediction = model.predict(X)
            
#             # Log or output the prediction
#             # print(f"Prediction for data {data['CustomerID'].values[0]}: {prediction[-1][0]}")
#             if len(data_buffer) >= buffer_size:
#                 # Combine data from the buffer into a single DataFrame
#                 combined_data = pd.concat(data_buffer)

#                 # Preprocess data
#                 combined_data = preprocess_data(combined_data)

#                 # Format data for prediction
#                 X = format_for_prediction(combined_data, time_step)

#                 # Make prediction
#                 prediction = model.predict(X)

#                 # # Log or output the prediction
#                 print(f"Prediction for data {combined_data['CustomerID'].values[-1]}: {prediction[-1][0]}")

#                 # Clear the buffer
#                 data_buffer.clear()
                
#         except Exception as e:
#             print(f"Error processing message {message}: {e}")

