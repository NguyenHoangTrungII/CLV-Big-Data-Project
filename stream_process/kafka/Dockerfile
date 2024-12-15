# Sử dụng OpenJDK 8
FROM openjdk:8-jdk

# Cài đặt Kafka
ENV KAFKA_VERSION=2.6.0
ENV SCALA_VERSION=2.12

# Tải và giải nén Kafka
RUN wget https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && tar -xvzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} /usr/local/kafka

# Sao chép file cấu hình Kafka
# COPY ./kafka_config/server.properties /usr/local/kafka/config/server.properties

# Thiết lập các biến môi trường
ENV KAFKA_HOME=/usr/local/kafka
ENV PATH=$KAFKA_HOME/bin:$PATH

# Expose các cổng
EXPOSE 9093
EXPOSE 2181

# Khởi động Kafka
CMD ["/bin/bash", "-c", "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties"]
