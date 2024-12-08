#!/bin/bash

# HBase RegionServer Environment Configuration

# Cấu hình bộ nhớ cho RegionServer
export HBASE_HEAPSIZE=4096        # Kích thước bộ nhớ heap của JVM (4GB trong ví dụ này)

# Cấu hình bộ nhớ off-heap cho RegionServer
export HBASE_REGION_SERVER_OPTS="-XX:MaxDirectMemorySize=2048m"  # Bộ nhớ off-heap (2GB trong ví dụ này)

# Cấu hình heap size tối thiểu và tối đa của JVM
export HBASE_REGIONSERVER_HEAPSIZE=4g   # Đặt kích thước heap cho JVM, ví dụ 4GB

# Cấu hình cho các tham số garbage collection của JVM
export HBASE_GC_OPTS="-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/opt/hbase/logs/regionserver-gc.log"

# Cấu hình log4j cho HBase RegionServer
export HBASE_LOG_DIR="/opt/hbase/logs"  # Đường dẫn lưu trữ log cho RegionServer

# Cấu hình tên người dùng chạy HBase RegionServer
export HBASE_USER="hbase"  # Nếu cần, có thể đặt người dùng chạy HBase

# Cấu hình Zookeeper
export HBASE_ZOOKEEPER_QUORUM="zookeeper1,zookeeper2,zookeeper3"  # Quorum cho Zookeeper
export HBASE_ZOOKEEPER_CLIENT_PORT="2181"  # Cổng Zookeeper (mặc định 2181)

# Cấu hình các tham số cho việc kết nối đến HBase Master
export HBASE_MASTER="hbase-master"  # Tên máy chủ HBase Master

# Cấu hình thời gian chờ kết nối tới Zookeeper
export HBASE_ZOOKEEPER_SESSION_TIMEOUT="60000"  # Thời gian timeout kết nối tới Zookeeper (60s)

# Cấu hình số lượng Region tối đa mà RegionServer có thể mở
export HBASE_REGIONSERVER_MAX_REGIONS="1000"  # Đặt số lượng region tối đa

# Các tham số khác (có thể thêm hoặc thay đổi theo nhu cầu của bạn)
export HBASE_LOG_LEVEL="INFO"  # Mức độ log (DEBUG, INFO, WARN, ERROR)
