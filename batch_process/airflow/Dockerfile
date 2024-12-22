# # Sử dụng Python 3.10 làm base image (được Airflow hỗ trợ)
# FROM python:3.10-slim

# # Thiết lập các biến môi trường cho Airflow
# ENV AIRFLOW_HOME=/usr/local/airflow
# ENV AIRFLOW_VERSION=2.7.2
# ENV PYTHON_VERSION=3.10
# ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# # Cài đặt các gói cần thiết
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     python3-dev \
#     python3-distutils \
#     && rm -rf /var/lib/apt/lists/*

# # Tạo thư mục cho Airflow
# RUN mkdir -p $AIRFLOW_HOME
# WORKDIR $AIRFLOW_HOME

# # Cài đặt Apache Airflow với các ràng buộc
# RUN pip install "apache-airflow[postgres,celery]==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT_URL}

# RUN pip install pandas kafka-python tensorflow happybase hdfs pyspark numpy

# # Sao chép tệp cấu hình Airflow (nếu có)
# COPY ./airflow_config/airflow.cfg /usr/local/airflow/airflow.cfg

# # Expose cổng webserver của Airflow
# EXPOSE 8080

# Chạy Airflow webserver mặc định
# ENTRYPOINT ["bash", "-c", "airflow db init || true && airflow webserver --port 8080"]


# Sử dụng Ubuntu 22.04 làm base image
FROM ubuntu:22.04
# FROM nvidia/cuda:11.2.2-cudnn8-runtime-ubuntu20.04

# Thiết lập các biến môi trường cho Airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_VERSION=2.7.2
ENV PYTHON_VERSION=3.10
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ENV DEBIAN_FRONTEND=noninteractive

# Cài đặt các gói cần thiết và cài đặt Python 3.10 và JDK 8
RUN apt-get update && apt-get install -y \
    software-properties-common \
    wget \
    ca-certificates \
    curl \
    apt-transport-https \
    lsb-release \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y \
    python3.10 \
    python3.10-dev \
    python3.10-distutils \
    python3-pip \
    openjdk-8-jdk \
    tzdata && \
    rm -rf /var/lib/apt/lists/*

# # Cài đặt Python 3.10 và thiết lập làm mặc định
# RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 \
#     && python3 --version


# # Cài đặt các thư viện Python bổ sung
# RUN pip install html5lib

# Tạo thư mục cho Airflow
RUN mkdir -p $AIRFLOW_HOME
WORKDIR $AIRFLOW_HOME

# Cài đặt Apache Airflow với các ràng buộc
RUN pip install  "apache-airflow[postgres,celery]==${AIRFLOW_VERSION}" --constraint ${CONSTRAINT_URL}
# Sao chép tệp cấu hình Airflow (nếu có)
COPY ./airflow_config/airflow.cfg /usr/local/airflow/airflow.cfg

RUN pip install pandas kafka-python tensorflow happybase hdfs pyspark==3.3.4 numpy

ENV TZ=Asia/Ho_Chi_Minh
RUN echo "Asia/Ho_Chi_Minh" > /etc/timezone && \
    dpkg-reconfigure -f noninteractive tzdata

# Expose cổng webserver của Airflow
EXPOSE 8080

# Chạy Airflow webserver mặc định
# ENTRYPOINT ["bash", "-c", "airflow db init || true && airflow webserver --port 8080"]
