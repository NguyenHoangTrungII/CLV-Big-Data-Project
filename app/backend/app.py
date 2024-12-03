from flask import Flask, jsonify
import happybase
import threading
import time

app = Flask(__name__)

# Kết nối tới HBase
connection = happybase.Connection('localhost') 
table_name = 'clv_predictions_new'
table = connection.table(table_name)

# Bộ nhớ cache để lưu dữ liệu mới
latest_data = []

# Hàm đọc dữ liệu liên tục từ HBase
def fetch_data_from_hbase():
    global latest_data
    while True:
        rows = table.scan()  # Đọc tất cả dữ liệu
        # Xử lý từng hàng
        latest_data = []
        for key, value in rows:
            try:
                # Giải mã row_key và các cột từ bytes sang str
                decoded_row = {
                    "row_key": key.decode('utf-8'),
                    "data": {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
                }
                latest_data.append(decoded_row)
            except Exception as e:
                print(f"Error decoding row: {e}")
        time.sleep(2)  # Giảm tải cho server HBase

# Thread chạy liên tục
thread = threading.Thread(target=fetch_data_from_hbase)
thread.daemon = True
thread.start()

# API trả dữ liệu
@app.route('/data', methods=['GET'])
def get_data():
    return jsonify(latest_data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
