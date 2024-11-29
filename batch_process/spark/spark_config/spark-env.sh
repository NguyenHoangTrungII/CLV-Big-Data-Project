# Thiết lập các biến môi trường cho Spark
# export SPARK_MASTER_HOST=spark-master
# export SPARK_LOCAL_IP=localhost
export SPARK_MASTER_HOST=0.0.0.0
export SPARK_LOCAL_IP=0.0.0.0
export SPARK_NO_DAEMONIZE=1
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=false -Dspark.worker.register.timeout=600"
      
# export SPARK_MASTER_IP=$(ip route get 1.1.1.1 | awk '{print $NF;exit}') # Get the container's IP
# export SPARK_LOCAL_IP=$SPARK_MASTER_IP

    