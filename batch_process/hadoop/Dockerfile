FROM openjdk:8-jdk

# Cài đặt các dependencies cần thiết
RUN apt-get update && apt-get install -y \
    wget \
    openssh-server \
    curl \
    vim \
    gettext-base && \
    rm -rf /var/lib/apt/lists/*

# Tải và cài đặt Hadoop
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.0/hadoop-2.7.0.tar.gz && \
    tar -xvzf hadoop-2.7.0.tar.gz && \
    mv hadoop-2.7.0 /usr/local/hadoop && \
    rm hadoop-2.7.0.tar.gz

# Cấu hình SSH
RUN mkdir -p /var/run/sshd && \
    echo 'root:rootpassword' | chpasswd && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# Tạo khóa SSH và thiết lập quyền
RUN ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys

# Sao chép các tệp cấu hình Hadoop
COPY ./batch_process/hadoop/hadoop_config/hdfs-site.xml.template /usr/local/hadoop/etc/hadoop/hdfs-site.xml.template
COPY ./batch_process/hadoop/hadoop_config/core-site.xml.template /usr/local/hadoop/etc/hadoop/core-site.xml.template

# Đặt các biến môi trường
ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV DFS_REPLICATION=3
ENV NAMENODE_DIR="file:///usr/local/hadoop/data/name"
ENV DATANODE_DIR="file:///usr/local/hadoop/data/datanode"
ENV HADOOP_NAMENODE_HOST="namenode"
# ENV HADOOP_NAMENODE_HOST="172.23.217.20"
ENV HADOOP_NAMENODE_PORT="9000"
ENV PATH=$JAVA_HOME/bin:$PATH


# Thay thế các biến môi trường trong tệp cấu hình Hadoop
RUN envsubst < /usr/local/hadoop/etc/hadoop/hdfs-site.xml.template > /usr/local/hadoop/etc/hadoop/hdfs-site.xml
RUN envsubst < /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml
RUN echo "export JAVA_HOME=/usr/local/openjdk-8" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh

# Tạo thư mục dữ liệu và logs cho Hadoop
RUN mkdir -p /usr/local/hadoop/logs && \
    mkdir -p /usr/local/hadoop/data/name && \
    mkdir -p /usr/local/hadoop/data/datanode && \
    chown -R root:root /usr/local/hadoop

# Format NameNode (nếu chưa format trước đó)
# RUN $HADOOP_HOME/bin/hdfs namenode -format

#Sao chép file HBase Coprocessor jar vào container
COPY ./stream_process/hbase/hbase_config/hbase-coprocessor-example.jar /tmp/hbase-coprocessor-example.jar

COPY ./batch_process/hadoop/hadoop_config/init-hbase.sh /init-hbase.sh
RUN chmod +x /init-hbase.sh

# Thêm cấu hình HADOOP_NAMENODE_OPTS vào hadoop-env.sh
RUN echo "export HADOOP_NAMENODE_OPTS=\"-Djava.net.preferIPv4Stack=true -Dfs.defaultFS=hdfs://namenode:9000\"" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh

# Mở các cổng cần thiết
EXPOSE 22 50070 8088 9000
