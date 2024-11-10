#!/bin/bash

# Function to stop Kafka
stop_kafka() {
    echo "Stopping Kafka..."
    $KAFKA_HOME/bin/kafka-server-stop.sh
    sleep 2
}

# Function to stop YARN
stop_yarn() {
    echo "Stopping YARN..."
    $HADOOP_HOME/sbin/stop-yarn.sh
    sleep 2
}

# Function to stop HDFS
stop_hdfs() {
    echo "Stopping HDFS Namenode and Datanode..."
    $HADOOP_HOME/sbin/stop-dfs.sh
    sleep 2
}

# Function to stop ZooKeeper
stop_zookeeper() {
    echo "Stopping ZooKeeper..."
    sudo systemctl stop zookeeper
    sleep 2
}

# Stop all services
echo "Stopping all services..."
stop_kafka
stop_yarn
stop_hdfs
stop_zookeeper

echo "All services have been stopped!"
