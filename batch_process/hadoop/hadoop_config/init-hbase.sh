#!/bin/bash

# Đảm bảo rằng NameNode và DataNode đã khởi động
sleep 30  # Chờ một chút để các dịch vụ Hadoop khởi động

# Tạo thư mục HBase trên HDFS
$HADOOP_HOME/bin/hdfs dfs -mkdir /hbase
$HADOOP_HOME/bin/hdfs dfs -mkdir /hbase/regions
$HADOOP_HOME/bin/hdfs dfs -mkdir /hbase/wal

# Tải file coprocessor lên HDFS
$HADOOP_HOME/bin/hdfs dfs -put /tmp/hbase-coprocessor-example.jar /user/hbase/coprocessors/
