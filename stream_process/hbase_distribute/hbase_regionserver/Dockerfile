# Sử dụng image OpenJDK 8 làm base image (HBase yêu cầu Java)
FROM openjdk:8-jdk

# Cài đặt các công cụ cần thiết
RUN apt-get update && \
    apt-get install -y wget bash && \
    rm -rf /var/lib/apt/lists/*  # Giảm dung lượng image

# Tải xuống và giải nén HBase
RUN wget https://archive.apache.org/dist/hbase/1.2.6/hbase-1.2.6-bin.tar.gz && \
    tar -xvzf hbase-1.2.6-bin.tar.gz && \
    mv hbase-1.2.6 /usr/local/hbase && \
    rm hbase-1.2.6-bin.tar.gz  # Xoá file tải về để giảm dung lượng image

# Cấu hình các tệp cần thiết cho HBase RegionServer
COPY ./stream_process/hbase_distribute/hbase_regionserver/hbase_config/hbase-site.xml /usr/local/hbase/conf/
COPY ./stream_process/hbase_distribute/hbase_regionserver/hbase_config/core-site.xml /usr/local/hbase/conf/
COPY ./stream_process/hbase_distribute/hbase_coprocessors/hbase-coprocessor-example.jar /opt/hbase/coprocessors/

# Sửa file hbase-env.sh để loại bỏ lỗi PermSize
RUN sed -i 's/-XX:PermSize=128m -XX:MaxPermSize=128m/-Xms512m -Xmx1024m/g' /usr/local/hbase/conf/hbase-env.sh

# Tạo và cấp quyền cho thư mục dữ liệu
RUN mkdir -p /usr/local/hbase/zookeeper && \
    chmod -R 755 /usr/local/hbase && \
    chmod -R 755 /usr/local/hbase/zookeeper

# Thiết lập các biến môi trường cho HBase
ENV HBASE_HOME=/usr/local/hbase
ENV PATH=$PATH:$HBASE_HOME/bin:$HBASE_HOME/sbin

# Cấu hình các tham số môi trường cho RegionServer
COPY ./stream_process/hbase_distribute/hbase_regionserver/hbase_config/regionserver-env.sh /usr/local/hbase/conf/regionserver-env.sh

# Cấu hình các tham số cho HBase RegionServer và Master thông qua environment variables
ENV HBASE_MASTER="hbase-master"  
# ENV HBASE_ZOOKEEPER_QUORUM="zookeeper1,zookeeper2,zookeeper3"  
ENV HBASE_ZOOKEEPER_CLIENT_PORT="2181" 

# Expose các cổng cần thiết cho HBase
EXPOSE 16020 9090