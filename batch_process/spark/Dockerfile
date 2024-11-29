# Sử dụng image OpenJDK 8 làm nền tảng (vì Spark yêu cầu Java)
FROM openjdk:8-jdk

# Cài đặt wget, python3-pip và curl
RUN apt-get update && \
    apt-get install -y wget python3-pip rsync netcat && \
    pip3 install pyspark

# Tải xuống và giải nén Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop2.tgz && \
    tar -xvzf spark-3.3.4-bin-hadoop2.tgz && \
    mv spark-3.3.4-bin-hadoop2 /usr/local/spark

# Thiết lập các biến môi trường cho Spark
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Thiết lập JAVA_HOME cho Spark
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$JAVA_HOME/bin:$PATH

# Thiết lập SPARK_MASTER_HOST cho Spark
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_LOCAL_IP=localhost

# Chuyển các tệp cấu hình Spark vào container (nếu có)
COPY ./spark_config/spark-defaults.conf /usr/local/spark/conf/
COPY ./spark_config/spark-env.sh /usr/local/spark/conf/

# Expose các cổng cần thiết cho Spark
EXPOSE 4040 7077 8080 8081

# Lệnh khởi động Spark master trong chế độ foreground
# CMD ["/usr/local/spark/sbin/start-master.sh", "org.apache.spark.deploy.master.Master"]
