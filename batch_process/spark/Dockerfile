# Sử dụng image OpenJDK 8 làm nền tảng (vì Spark yêu cầu Java)
FROM openjdk:8-jdk

# Cài đặt các gói phụ trợ cần thiết để biên dịch Python từ mã nguồn
RUN apt-get update && \
    apt-get install -y wget python3-pip rsync netcat \
    build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev \
    libssl-dev libreadline-dev libffi-dev curl

# Tải mã nguồn Python 3.10 và biên dịch
RUN wget https://www.python.org/ftp/python/3.10.10/Python-3.10.10.tgz && \
    tar -xf Python-3.10.10.tgz && \
    cd Python-3.10.10 && \
    ./configure --enable-optimizations && \
    make && \
    make altinstall

# Cài đặt pip cho Python 3.10
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

# Tải xuống và giải nén Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop2.tgz && \
    tar -xvzf spark-3.3.4-bin-hadoop2.tgz && \
    mv spark-3.3.4-bin-hadoop2 /usr/local/spark

# Thiết lập các biến môi trường cho Spark
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/usr/local/bin/python3.10
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10

# Thiết lập JAVA_HOME cho Spark
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$JAVA_HOME/bin:$PATH

# Thiết lập SPARK_MASTER_HOST cho Spark
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_LOCAL_IP=localhost


# Chuyển các tệp cấu hình Spark vào container (nếu có)
COPY ./spark_config/spark-defaults.conf /usr/local/spark/conf/
COPY ./spark_config/spark-env.sh /usr/local/spark/conf/
COPY ./spark_config/fair-scheduler.xml /usr/local/spark/conf/

# Expose các cổng cần thiết cho Spark
EXPOSE 4040 7077 8080 8081
