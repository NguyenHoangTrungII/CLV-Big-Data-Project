
#!/bin/bash
# Thêm thư mục /app vào PYTHONPATH
export PYTHONPATH="/app:$PYTHONPATH"

# Khởi chạy ứng dụng chính
python3 -m stream_process.stream_pipeline

# Khởi chạy ứng dụng chính
python3 -m stream_process.stream_pipeline
