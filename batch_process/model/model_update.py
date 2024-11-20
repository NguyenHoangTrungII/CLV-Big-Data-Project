from tensorflow  import keras 

# Định nghĩa early stopping
early_stop = keras.callbacks.EarlyStopping(monitor='val_loss', patience=20)

# Tải mô hình đã huấn luyện trước đó (nếu có)
model = None
try:
    model = keras.models.load_model('/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras', compile=False)
    print("Model loaded successfully.")
except Exception as e:
    print(f"Error loading model: {e}")
    exit(1)

# Kiểm tra cấu trúc mô hình
model.summary()

# Đóng băng các lớp ngoài lớp output nếu bạn muốn fine-tune
for layer in model.layers[:-1]:  # Đóng băng tất cả lớp ngoài lớp output
    layer.trainable = False

# Fine-tuning: Huấn luyện lại mô hình với dữ liệu mới
EPOCHS = 100

history = model.fit(X_train, y_train,
                    epochs=EPOCHS,
                    validation_split=0.2,
                    verbose=1,
                    callbacks=[early_stop])

# Lưu lại mô hình đã fine-tuned
model.save('fine_tuned_model.h5')