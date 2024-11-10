#!/bin/bash

# Function to start ZooKeeper
start_zookeeper() {
    echo "Starting ZooKeeper..."
    sudo systemctl start zookeeper
    sleep 2
}

# Function to start Hadoop HDFS
start_hdfs() {
    echo "Starting HDFS Namenode and Datanode..."
    $HADOOP_HOME/sbin/start-dfs.sh
    sleep 2
}

# Function to start YARN
start_yarn() {
    echo "Starting YARN ResourceManager and NodeManager..."
    $HADOOP_HOME/sbin/start-yarn.sh
    sleep 2
}

# Function to start Kafka
start_kafka() {
    echo "Starting Kafka..."
    $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
    sleep 2
}

# Start all services
echo "Starting all services..."
start_zookeeper
start_hdfs
start_yarn
start_kafka

echo "All services have been started!"
