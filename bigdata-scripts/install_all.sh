
# #!/bin/bash

# # Cập nhật hệ thống
# echo "Updating system packages..."
# sudo apt-get update -y

# # Cài đặt Java (Yêu cầu cho Hadoop, Spark, HBase, Kafka)
# echo "Installing Java..."
# sudo apt-get install -y openjdk-8-jdk

# # Cài đặt Zookeeper (Yêu cầu cho Kafka)
# echo "Installing Zookeeper..."
# sudo apt-get install -y zookeeperd

# # Cài đặt Apache Kafka (Version 2.6.0)
# echo "Installing Apache Kafka 2.6.0..."
# cd /usr/local
# sudo wget https://archive.apache.org/dist/kafka/2.6.0/kafka_2.13-2.6.0.tgz
# sudo tar -xvzf kafka_2.13-2.6.0.tgz
# sudo ln -s /usr/local/kafka_2.13-2.6.0 /usr/local/kafka
# echo "Kafka installation completed."

# # Cài đặt Apache HBase (Version 1.2.6)
# echo "Installing Apache HBase 1.2.6..."
# cd /usr/local
# sudo wget https://archive.apache.org/dist/hbase/1.2.6/hbase-1.2.6-bin.tar.gz
# sudo tar -xvzf hbase-1.2.6-bin.tar.gz
# sudo ln -s /usr/local/hbase-1.2.6 /usr/local/hbase
# echo "HBase installation completed."

# # Cài đặt Apache Hadoop (Version 2.7.0)
# echo "Installing Apache Hadoop 2.7.0..."
# cd /usr/local
# sudo wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz
# sudo tar -xvzf hadoop-2.7.0.tar.gz
# sudo ln -s /usr/local/hadoop-2.7.0 /usr/local/hadoop
# echo "Hadoop installation completed."

# # Cài đặt Apache Spark (Version 3.3.4)
# echo "Installing Apache Spark 3.3.4..."
# cd /usr/local
# sudo wget https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz
# sudo tar -xvzf spark-3.3.4-bin-hadoop3.tgz
# sudo ln -s /usr/local/spark-3.3.4-bin-hadoop3 /usr/local/spark
# echo "Spark installation completed."

# # Thiết lập các biến môi trường
# echo "Setting up environment variables..."
# echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' | sudo tee -a /etc/profile.d/java.sh
# echo 'export HADOOP_HOME=/usr/local/hadoop' | sudo tee -a /etc/profile.d/hadoop.sh
# echo 'export SPARK_HOME=/usr/local/spark' | sudo tee -a /etc/profile.d/spark.sh
# echo 'export HBASE_HOME=/usr/local/hbase' | sudo tee -a /etc/profile.d/hbase.sh
# echo 'export KAFKA_HOME=/usr/local/kafka' | sudo tee -a /etc/profile.d/kafka.sh

# # Tải lại các biến môi trường
# source /etc/profile.d/java.sh
# source /etc/profile.d/hadoop.sh
# source /etc/profile.d/spark.sh
# source /etc/profile.d/hbase.sh
# source /etc/profile.d/kafka.sh

# echo "Installation of all components completed."

# # Khởi động các dịch vụ
# echo "Starting Zookeeper..."
# sudo systemctl start zookeeper

# echo "Starting Kafka..."
# # Cấu hình Kafka với Zookeeper nếu cần và khởi động
# sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

# echo "Starting HBase..."
# # Cấu hình HBase nếu cần và khởi động
# sudo /usr/local/hbase/bin/start-hbase.sh

# echo "Starting Hadoop..."
# # Cấu hình Hadoop nếu cần và khởi động
# sudo /usr/local/hadoop/sbin/start-dfs.sh
# sudo /usr/local/hadoop/sbin/start-yarn.sh

# echo "Starting Spark..."
# # Cấu hình Spark nếu cần và khởi động
# sudo /usr/local/spark/sbin/start-all.sh

# echo "All components installed and started successfully."


#!/bin/bash

# Cập nhật hệ thống
echo "Updating system packages..."
sudo apt-get update -y

# Kiểm tra và cài đặt Java
echo "Checking if Java is installed..."
if ! java -version &>/dev/null; then
    echo "Java is not installed. Installing Java..."
    sudo apt-get install -y openjdk-8-jdk
else
    echo "Java is already installed."
fi

# Kiểm tra và cài đặt Zookeeper
echo "Checking if Zookeeper is installed..."
if ! dpkg -l | grep -q zookeeperd; then
    echo "Zookeeper is not installed. Installing Zookeeper..."
    sudo apt-get install -y zookeeperd
else
    echo "Zookeeper is already installed."
fi

# Kiểm tra và cài đặt Apache Kafka
echo "Checking if Apache Kafka is installed..."
if [ ! -d "/usr/local/kafka" ]; then
    echo "Apache Kafka is not installed. Installing Kafka..."
    cd /usr/local
    sudo wget https://archive.apache.org/dist/kafka/2.6.0/kafka_2.13-2.6.0.tgz
    sudo tar -xvzf kafka_2.13-2.6.0.tgz
    sudo ln -s /usr/local/kafka_2.13-2.6.0 /usr/local/kafka
    echo "Kafka installation completed."
else
    echo "Apache Kafka is already installed."
fi

# Kiểm tra và cài đặt Apache HBase
echo "Checking if Apache HBase is installed..."
if [ ! -d "/usr/local/hbase" ]; then
    echo "Apache HBase is not installed. Installing HBase..."
    cd /usr/local
    sudo wget https://archive.apache.org/dist/hbase/1.2.6/hbase-1.2.6-bin.tar.gz
    sudo tar -xvzf hbase-1.2.6-bin.tar.gz
    sudo ln -s /usr/local/hbase-1.2.6 /usr/local/hbase
    echo "HBase installation completed."
else
    echo "Apache HBase is already installed."
fi

# Kiểm tra và cài đặt Apache Hadoop
echo "Checking if Apache Hadoop is installed..."
if [ ! -d "/usr/local/hadoop" ]; then
    echo "Apache Hadoop is not installed. Installing Hadoop..."
    cd /usr/local
    sudo wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz
    sudo tar -xvzf hadoop-2.7.0.tar.gz
    sudo ln -s /usr/local/hadoop-2.7.0 /usr/local/hadoop
    echo "Hadoop installation completed."
else
    echo "Apache Hadoop is already installed."
fi

# Kiểm tra và cài đặt Apache Spark
echo "Checking if Apache Spark is installed..."
if [ ! -d "/usr/local/spark" ]; then
    echo "Apache Spark is not installed. Installing Spark..."
    cd /usr/local
    sudo wget https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz
    sudo tar -xvzf spark-3.3.4-bin-hadoop3.tgz
    sudo ln -s /usr/local/spark-3.3.4-bin-hadoop3 /usr/local/spark
    echo "Spark installation completed."
else
    echo "Apache Spark is already installed."
fi

echo "Checking if PostgreSQL is installed..."
if ! dpkg -l | grep -q postgresql; then
    echo "PostgreSQL is not installed. Installing PostgreSQL..."
    sudo apt-get install -y postgresql postgresql-contrib
    echo "PostgreSQL installation completed."
else
    echo "PostgreSQL is already installed."
fi

# Thiết lập các biến môi trường
echo "Setting up environment variables..."
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' | sudo tee -a /etc/profile.d/java.sh
echo 'export HADOOP_HOME=/usr/local/hadoop' | sudo tee -a /etc/profile.d/hadoop.sh
echo 'export SPARK_HOME=/usr/local/spark' | sudo tee -a /etc/profile.d/spark.sh
echo 'export HBASE_HOME=/usr/local/hbase' | sudo tee -a /etc/profile.d/hbase.sh
echo 'export KAFKA_HOME=/usr/local/kafka' | sudo tee -a /etc/profile.d/kafka.sh

# Tải lại các biến môi trường
source /etc/profile.d/java.sh
source /etc/profile.d/hadoop.sh
source /etc/profile.d/spark.sh
source /etc/profile.d/hbase.sh
source /etc/profile.d/kafka.sh

echo "Installation of all components completed."

# Khởi động các dịch vụ
echo "Starting Zookeeper..."
sudo systemctl start zookeeper

echo "Starting Kafka..."
# Cấu hình Kafka với Zookeeper nếu cần và khởi động
sudo /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties &

echo "Starting HBase..."
# Cấu hình HBase nếu cần và khởi động
sudo /usr/local/hbase/bin/start-hbase.sh

echo "Starting Hadoop..."
# Cấu hình Hadoop nếu cần và khởi động
sudo /usr/local/hadoop/sbin/start-dfs.sh
sudo /usr/local/hadoop/sbin/start-yarn.sh

echo "Starting Spark..."
# Cấu hình Spark nếu cần và khởi động
sudo /usr/local/spark/sbin/start-all.sh

echo "All components installed and started successfully."
