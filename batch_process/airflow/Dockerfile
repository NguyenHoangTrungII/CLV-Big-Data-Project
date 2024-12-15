# Sử dụng Python 3.10 làm base image (được Airflow hỗ trợ)
FROM python:3.10-slim

# Thiết lập các biến môi trường cho Airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_VERSION=2.7.2
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Cài đặt các gói cần thiết
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    python3-distutils \
    && rm -rf /var/lib/apt/lists/*

# Tạo thư mục cho Airflow
RUN mkdir -p $AIRFLOW_HOME
WORKDIR $AIRFLOW_HOME

# Cài đặt Apache Airflow với các ràng buộc
RUN pip install "apache-airflow[postgres,celery]==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT_URL}

# Sao chép tệp cấu hình Airflow (nếu có)
COPY ./airflow_config/airflow.cfg $AIRFLOW_HOME/airflow.cfg

# Expose cổng webserver của Airflow
EXPOSE 8080

# Chạy Airflow webserver mặc định
ENTRYPOINT ["bash", "-c", "airflow db init || true && airflow webserver --port 8080"]
