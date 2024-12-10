# from flask import Flask
# from flask_socketio import SocketIO, emit
# import happybase
# import time
# import threading

# # Khởi tạo Flask và SocketIO
# app = Flask(__name__)
# socketio = SocketIO(app, cors_allowed_origins="*")

# # Kết nối tới HBase
# connection = happybase.Connection('localhost')
# table_name = 'clv_predictions_new'
# table = connection.table(table_name)

# # Hàm lấy dữ liệu CLV từ HBase
# def get_clv_data():
#     # Lấy 10 bản ghi gần nhất từ bảng
#     rows = table.scan(limit=10)  # Bạn có thể thay đổi logic tùy thuộc vào cách bạn lưu trữ dữ liệu trong HBase
#     clv_data = []
#     for key, data in rows:
#         clv_data.append({
#             'customer_id': key.decode('utf-8'),  # key sẽ là customer_id
#             'clv': int(data.get(b'cf:clv', 0)),  # Giả sử giá trị CLV lưu trong column family 'cf', column 'clv'
#             'timestamp': data.get(b'cf:timestamp', b'').decode('utf-8')
#         })
#     return clv_data

# # Hàm phát dữ liệu CLV theo thời gian thực
# def emit_clv_data():
#     while True:
#         # Lấy dữ liệu CLV từ HBase
#         clv_data = get_clv_data()
        
#         # Gửi dữ liệu CLV đến frontend qua SocketIO
#         for data in clv_data:
#             emit('new_clv_data', data, broadcast=True)
        
#         # Chờ 2 giây để gửi dữ liệu tiếp theo
#         time.sleep(2)

# # Route chính
# @app.route('/')
# def index():
#     return "Flask Server for CLV Data"

# # Khởi động luồng gửi dữ liệu CLV
# # @app.before_request
# # def before_request():
# #     thread = threading.Thread(target=emit_clv_data)
# #     thread.daemon = True
# #     thread.start()

# @socketio.on('connect')
# def start_clv_data_stream():
#     # Bắt đầu chạy luồng nền khi có kết nối từ frontend
#     socketio.start_background_task(target=emit_clv_data)

# if __name__ == '__main__':
#     socketio.run(app, host='0.0.0.0', port=3000)


# from flask import Flask
# from flask_socketio import SocketIO, emit
# import happybase
# import time
# import threading

# # Khởi tạo Flask và SocketIO
# app = Flask(__name__)
# socketio = SocketIO(app, cors_allowed_origins="*")

# # Kết nối tới HBase
# connection = happybase.Connection('localhost')
# table_name = 'clv_predictions_new'
# table = connection.table(table_name)

# # Hàm lấy dữ liệu CLV từ HBase
# def get_clv_data():
#     # Lấy 10 bản ghi gần nhất từ bảng
#     rows = table.scan(limit=10)  # Bạn có thể thay đổi logic tùy thuộc vào cách bạn lưu trữ dữ liệu trong HBase
#     clv_data = []
#     for key, data in rows:
#         clv_data.append({
#             'customer_id': key.decode('utf-8'),  # key sẽ là customer_id
#             'clv': int(data.get(b'cf:clv', 0)),  # Giả sử giá trị CLV lưu trong column family 'cf', column 'clv'
#             'timestamp': data.get(b'cf:timestamp', b'').decode('utf-8')
#         })
#     return clv_data

# # Hàm phát dữ liệu CLV theo thời gian thực
# def emit_clv_data():
#     while True:
#         # Lấy dữ liệu CLV từ HBase
#         clv_data = get_clv_data()
        
#         # Gửi dữ liệu CLV đến tất cả các client qua SocketIO
#         for data in clv_data:
#             socketio.emit('new_clv_data', data, broadcast=True)  # Sử dụng broadcast đúng cách
        
#         # Chờ 2 giây để gửi dữ liệu tiếp theo
#         time.sleep(2)

# # Route chính
# @app.route('/')
# def index():
#     return "Flask Server for CLV Data"

# # Khởi động luồng gửi dữ liệu CLV khi server bắt đầu chạy
# @socketio.on('connect')
# def start_clv_data_stream():
#     # Bắt đầu chạy luồng nền khi có kết nối từ frontend
#     socketio.start_background_task(target=emit_clv_data)

# if __name__ == '__main__':
#     socketio.run(app, host='0.0.0.0', port=3000)

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
