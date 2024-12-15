from flask import Flask
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Cấu hình Kafka Consumer
KAFKA_BROKER = '172.27.254.108:9093'  # Địa chỉ Kafka broker của bạn
KAFKA_TOPIC = 'hbase-clv-topic'  # Tên Kafka topic bạn muốn tiêu thụ
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id='clv-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Hàm gửi dữ liệu CLV real-time từ Kafka tới client
def emit_clv_data():
    for message in consumer:
        data = message.value  # Lấy dữ liệu từ Kafka
        # Gửi dữ liệu qua SocketIO
        socketio.emit('new_clv_data', data)

# Khi client kết nối
@socketio.on('connect')
def handle_connect():
    print("Client connected")
    # Chạy luồng nền để gửi dữ liệu từ Kafka
    socketio.start_background_task(target=emit_clv_data)

# Khi client ngắt kết nối
@socketio.on('disconnect')
def handle_disconnect():
    print("Client disconnected")

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=3000, debug=True)
