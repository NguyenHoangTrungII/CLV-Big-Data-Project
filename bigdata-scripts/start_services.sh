#!/bin/bash

# Thiết lập các biến môi trường
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}
export KAFKA_HOME=${KAFKA_HOME:-/usr/local/kafka}
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$KAFKA_HOME/bin

# Kiểm tra biến môi trường
if [ -z "$HADOOP_HOME" ] || [ -z "$KAFKA_HOME" ]; then
    echo "Error: Please make sure HADOOP_HOME and KAFKA_HOME are set."
    exit 1
fi

# Khởi động ZooKeeper
echo "Starting ZooKeeper..."
sudo systemctl start zookeeper
sleep 2

# Khởi động Hadoop HDFS
echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh
sleep 2

# Khởi động YARN
echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh
sleep 2

# Khởi động Kafka
echo "Starting Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 2

echo "All services started successfully!"

