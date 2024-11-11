# -*- coding: utf-8 -*-
"""
CLV-Big-Data-Project
"""
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense
import tensorflow as tf

# --- Phần 1: Chuẩn bị dữ liệu từ Google Drive ---
drive.mount('/content/drive')

# Đường dẫn tới tệp Excel trong Google Drive
file_path = './data/raw/Online_Retail.xlsx'  


# Đọc dữ liệu từ tệp Excel
df = pd.read_excel(file_path)
print("Dữ liệu ban đầu:")
print(df.head())

# --- Phần 2: Làm sạch dữ liệu ---
# Chuyển đổi cột 'InvoiceDate' sang định dạng datetime
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Kiểm tra tổng số giá trị null
data_null_total = pd.DataFrame(df.isna().sum()).T.rename({0: 'Total Null'})
print("Số lượng giá trị null:")
print(data_null_total)

# Loại bỏ các hàng thiếu CustomerID
df_cleaned = df.dropna(subset=['CustomerID'])
df_cleaned['CustomerID'] = df_cleaned['CustomerID'].astype(str)

# Lọc bỏ các giá trị không hợp lệ
df_cleaned = df_cleaned[(df_cleaned['UnitPrice'] > 0) & (df_cleaned['Quantity'] > 0)]
print(f"Số hàng còn lại sau khi làm sạch: {df_cleaned.shape[0]}")

print("Summary Statistics:")
print(df_cleaned.describe())

# --- Phần 3: Tính toán TotalValue và giảm mẫu dữ liệu ---
df_cleaned['TotalValue'] = df_cleaned['Quantity'] * df_cleaned['UnitPrice']

# Giảm mẫu dữ liệu nếu số lượng bản ghi lớn hơn 10,000
if len(df_cleaned) > 10000:
    df_cleaned = df_cleaned.sample(10000, random_state=42).reset_index(drop=True)

# Nhóm theo InvoiceNo và tính tổng CLV
clv_data = df_cleaned.groupby('InvoiceNo').agg({'TotalValue': 'sum'}).reset_index()
print("Dữ liệu CLV đã tổng hợp:")
print(clv_data.head())

# --- Phần 4: Tiền xử lý dữ liệu cho mô hình ---
# Chuẩn hóa dữ liệu
scaler = MinMaxScaler(feature_range=(0, 1))
scaled_data = scaler.fit_transform(clv_data['TotalValue'].values.reshape(-1, 1))

# Chia dữ liệu thành train (60%), validation (20%), và test (20%)
train_size = int(len(scaled_data) * 0.6)
val_size = int(len(scaled_data) * 0.2)
test_size = len(scaled_data) - train_size - val_size

train_data = scaled_data[:train_size]
val_data = scaled_data[train_size:train_size + val_size]
test_data = scaled_data[train_size + val_size:]

# --- Phần 5: Tạo tập dữ liệu cho mô hình ---
def create_dataset(data, time_step=1):
    X, y = [], []
    for i in range(len(data) - time_step):
        X.append(data[i:(i + time_step), 0])
        y.append(data[i + time_step, 0])
    return np.array(X), np.array(y)

# Định nghĩa bước thời gian
time_step = 1

# Tạo tập train, val và test
X_train, y_train = create_dataset(train_data, time_step)
X_val, y_val = create_dataset(val_data, time_step)
X_test, y_test = create_dataset(test_data, time_step)

# Reshape để phù hợp với đầu vào của RNN
X_train = X_train.reshape(X_train.shape[0], X_train.shape[1], 1)
X_val = X_val.reshape(X_val.shape[0], X_val.shape[1], 1)
X_test = X_test.reshape(X_test.shape[0], X_test.shape[1], 1)

# --- Phần 6: Xây dựng và huấn luyện mô hình ---
model = Sequential([
    LSTM(50, return_sequences=True, input_shape=(X_train.shape[1], 1)),
    LSTM(50),
    Dense(1)
])

# Biên dịch mô hình
model.compile(optimizer='adam', loss='mean_squared_error')

# Huấn luyện mô hình với tập validation
history = model.fit(X_train, y_train, epochs=50, batch_size=1, validation_data=(X_val, y_val), verbose=2)

# --- Phần 7: Dự đoán và đánh giá mô hình ---
predicted_clv = model.predict(X_test)
predicted_clv = scaler.inverse_transform(predicted_clv)

# So sánh với y_test (giá trị thật của tập test)
y_test_actual = scaler.inverse_transform(y_test.reshape(-1, 1))

# Tính toán các chỉ số đánh giá
mae = np.mean(np.abs(predicted_clv - y_test_actual))
mse = np.mean((predicted_clv - y_test_actual) ** 2)
rmse = np.sqrt(mse)

print(f'Predicted CLV: {predicted_clv.flatten()}')
print(f'MAE: {mae}')
print(f'MSE: {mse}')
print(f'RMSE: {rmse}')

# --- Phần 8: Lưu mô hình ---
model.save("CLV.h5")

# --- Kiểm tra dữ liệu ---
print("X_train shape:", X_train.shape)
print("y_test shape:", y_test.shape)
print("train_data shape:", train_data.shape)
print("test_data shape:", test_data.shape)
print("Test size:", test_size)
