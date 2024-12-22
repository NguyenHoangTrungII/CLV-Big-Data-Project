# Sử dụng image OpenJDK 8 làm base image (HBase yêu cầu Java)
FROM openjdk:8-jdk

# Cài đặt các công cụ cần thiết
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get install -y bash

# Tải xuống và giải nén HBase
RUN wget https://archive.apache.org/dist/hbase/1.2.6/hbase-1.2.6-bin.tar.gz && \
    tar -xvzf hbase-1.2.6-bin.tar.gz && \
    mv hbase-1.2.6 /usr/local/hbase

# Cấu hình các tệp cần thiết
COPY ./stream_process/hbase/hbase_config/hbase-site.xml /usr/local/hbase/conf/
COPY ./stream_process/hbase/hbase_config/core-site.xml /usr/local/hbase/conf/

# Copy the coprocessor jar file to the HBase container
# COPY ./stream_process/hbase/hbase_coprocessors/hbase-coprocessor-example.jar /opt/hbase/coprocessors/

RUN echo "export HBASE_CLASSPATH=\$HBASE_CLASSPATH:/opt/hbase/coprocessors/hbase-coprocessor-example.jar" >> /usr/local/hbase/conf/hbase-env.sh

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
COPY ./stream_process/hbase/hbase_config/regionserver-env.sh /usr/local/hbase/conf/regionserver-env.sh

# Cấu hình các tham số cho HBase Master và RegionServer
# Cấu hình thông qua environment variables (hoặc có thể trực tiếp trong hbase-site.xml)
ENV HBASE_MASTER="hbase-master"  
ENV HBASE_ZOOKEEPER_QUORUM="zookeeper1,zookeeper2,zookeeper3"  
ENV HBASE_ZOOKEEPER_CLIENT_PORT="2181" 

# Expose các cổng cần thiết cho HBase (tùy chỉnh theo nhu cầu)
EXPOSE 2181 16010 16000 9090

