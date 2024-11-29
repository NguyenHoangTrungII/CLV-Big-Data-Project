FROM python:3.10-slim

# Thiết lập biến môi trường
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Cài đặt các công cụ cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    python3-dev \
    libffi-dev \
    libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cập nhật pip, setuptools, wheel
RUN pip install --upgrade pip setuptools wheel

# Cài đặt các phụ thuộc cần thiết trước
RUN pip install Cython "tornado<6.0" "six<2.0" "future<1.0"

# Cài đặt thriftpy2 từ mã nguồn
RUN pip install --no-binary :all: thriftpy2

# Đặt thư mục làm việc trong container
WORKDIR /app

# Sao chép mã nguồn vào thư mục làm việc /app
COPY . /app

# Sao chép file requirements.txt vào trong container và cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --default-timeout=300 --no-cache-dir -r requirements.txt

# Sao chép script vào container và cấp quyền thực thi
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# Lệnh mặc định để chạy
# CMD ["./run.sh"]
