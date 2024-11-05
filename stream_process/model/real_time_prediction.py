import joblib
import pandas as pd
import numpy as np

# Load the pre-trained CLV model
try:
    model = joblib.load('/home/nhtrung/product_recommendation_system/stream_process/model/CLV_model.pkl')
    print("Model loaded successfully.")
except Exception as e:
    print(f"Error loading model: {e}")

# Sample data similar to Kafka message format for testing
sample_data = {
    'CustomerID': [12345],
    'InvoiceDate': ['2023-01-01 10:00:00'],
    'Quantity': [5],
    'UnitPrice': [20.0]
}
data = pd.DataFrame(sample_data)

# Preprocess data
try:
    data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate']).astype('int64') // 10**9
    data['TotalValue'] = data['Quantity'] * data['UnitPrice']
    print("Data preprocessed successfully.")
except Exception as e:
    print(f"Error in preprocessing: {e}")

# Format data for LSTM prediction
try:
    time_step = 5
    data_array = data['TotalValue'].values.reshape(-1, 1)
    X, _ = [], []
    for i in range(len(data_array) - time_step):
        X.append(data_array[i:i + time_step, 0])
    X = np.array(X).reshape(X.shape[0], X.shape[1], 1)
    print("Data formatted for prediction.")
except Exception as e:
    print(f"Error in data formatting: {e}")

# Perform a test prediction
try:
    prediction = model.predict(X)
    print("Prediction successful:", prediction)
except Exception as e:
    print(f"Prediction error: {e}")
