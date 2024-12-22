from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, from_json, explode, col, regexp_replace, to_timestamp, hour, dayofweek, when, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, BooleanType,IntegerType, FloatType
from pyspark.sql import functions as F
from datetime import datetime
from tensorflow import keras

from batch_process.postgres.save_preprocessed_data import save_data_to_postgresql, save_to_postgres
from batch_process.model.model_update import load_and_finetune_model


import os
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load the model ONCE (outside any threads)
model = None
try:
    model = keras.models.load_model('./batch_process/model/clv/CLV_V3.keras', compile=False)
    logger.info("Model loaded successfully.")
except Exception as e:
    logger.error(f"Error loading model: {e}")
    exit(1)

def create_spark_session():
    """
    Create a SparkSession.
    """

    os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.10"
    os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3.10"

    spark = SparkSession.builder \
    .appName("CLV Prediction") \
    .master("spark://172.31.56.16:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    \
    .config("spark.jars", "./batch_process/spark/jar/postgresql-42.7.4.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    \
    .config("hbase.zookeeper.quorum", "172.19.0.7:2181") \
    .config("zookeeper.znode.parent", "/hbase") \
    .config("spark.driver.host", "172.31.56.16")\
    .config("spark.executor.heartbeatInterval", "60s")  \
    .config("spark.network.timeout", "120s")  \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.cores.max", "4")  \
    .config("spark.task.cpus", "1") \
    .config("spark.sql.shuffle.partitions", "100") \
    \
    .config("spark.dynamicAllocation.enabled", "false") \
    .getOrCreate()


    # spark = SparkSession.builder \
    #     .appName("CLV Prediction") \
    #     .master("spark://spark-master:7077") \
    #     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    #     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    #     .config("spark.driver.bindAddress", "0.0.0.0") \
    #     \
    #     .config("spark.jars", "./batch_process/spark/jar/postgresql-42.7.4.jar") \
    #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    #     \
    #     .config("hbase.zookeeper.quorum", "172.19.0.7:2181") \
    #     .config("zookeeper.znode.parent", "/hbase") \
    #     .config("spark.driver.host", "172.31.56.16")\
    #     .config("spark.executor.heartbeatInterval", "60s")  \
    #     .config("spark.network.timeout", "120s")  \
    #     .config("spark.executor.memory", "2g") \
    #     .config("spark.executor.cores", "2") \
    #     .config("spark.driver.memory", "2g") \
    #     .config("spark.cores.max", "4")  \
    #     .config("spark.task.cpus", "1") \
    #     .config("spark.sql.shuffle.partitions", "100") \
    #     \
    #     .config("spark.dynamicAllocation.enabled", "false") \
    #     .getOrCreate()
    

    return spark

def create_spark_session_v2():
    """
    Create a SparkSession.
    """

    os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.10"
    os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3.10"

    spark = SparkSession.builder \
    .appName("CLV Prediction IN airflow") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.host", "172.19.0.9") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.cores.max", "4") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.4.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("hbase.zookeeper.quorum", "zookeeper:2181") \
    .config("zookeeper.znode.parent", "/hbase") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .getOrCreate()

    return spark


# def create_spark_session(job_type: str):
#     """
#     Create a SparkSession.
#     """

#     os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.10"
#     os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/bin/python3.10"

#     spark = SparkSession.builder \
#     .appName(f"CLV Prediction - {job_type}") \
#     .master("spark://172.31.56.16:7077") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#     \
#     .config("spark.jars", "/home/nhtrung/CLV-Big-Data-Project/batch_process/spark/jar/postgresql-42.7.4.jar") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
#     \
#     .config("hbase.zookeeper.quorum", "localhost:2181") \
#     .config("zookeeper.znode.parent", "/hbase") \
#     .config("spark.driver.host", "172.31.56.16")\
#     .config("spark.executor.heartbeatInterval", "60s")  \
#     .config("spark.network.timeout", "120s")  \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.driver.memory", "2g") \
#     .config("spark.cores.max", "4")  \
#     .config("spark.task.cpus", "1") \
#     .config("spark.sql.shuffle.partitions", "100") \
#     \
#     .config("spark.shuffle.service.enabled", "true") \
#     \
#     .config("spark.dynamicAllocation.enabled", "true") \
#     .config("spark.dynamicAllocation.minExecutors", "2") \
#     .config("spark.dynamicAllocation.maxExecutors", "4") \
#     .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
#     \
#     .config("spark.scheduler.mode", "FAIR") \
#     \
#     .config("spark.scheduler.pool", job_type) \
#     .getOrCreate()

#     return spark


def read_data_from_hdfs(spark, file_path):
    """
    Read data from HDFS.
    Args:
        spark: SparkSession instance.
        file_path: Path to the file in HDFS.

    Returns:
        DataFrame containing the data.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def read_format_json_from_hdfs(spark, file_path):
    """
    Read data from HDFS.
    Args:
        spark: SparkSession instance.
        file_path: Path to the file in HDFS.

    Returns:
        DataFrame containing the data.
    """
    try:

        # spark = create_spark_session()

        # Định nghĩa schema tường minh cho dữ liệu
        schema = StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("InvoiceDate", StringType(), True),
            StructField("CustomerID", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Items", ArrayType(StructType([
                StructField("StockCode", StringType(), True),
                StructField("Description", StringType(), True),
                StructField("Quantity", FloatType(), True),
                StructField("UnitPrice", FloatType(), True)
            ])), True)
        ])

        raw_df = spark.read.option("multiline","true").schema(schema).json(file_path)
        print("Data from HDFS", raw_df.show())
        print("Data successfully read from HDFS.")
    except Exception as e:
        print(f"Error reading data from HDFS: {e}")
        return None
    
     # Explode the `Items` array into rows
    exploded_df = raw_df.withColumn("Items", raw_df["Items"]).selectExpr(
        "InvoiceNo", "InvoiceDate", "CustomerID", "Country", "explode(Items) as Items"
    )
    
    # Extract fields from `Items`
    transformed_df = exploded_df.select(
        "InvoiceNo",
        "InvoiceDate",
        "CustomerID",
        "Country",
        "Items.StockCode",
        "Items.Description",
        "Items.Quantity",
        "Items.UnitPrice"
    )
    

    # # Convert to a Python list of dictionaries for further processing
    transformed_rdd = transformed_df.rdd.map(lambda row: {
        'InvoiceNo': row.InvoiceNo,
        'StockCode': row.StockCode,
        'Description': row.Description,
        'Quantity': row.Quantity,
        'InvoiceDate': row.InvoiceDate,
        'UnitPrice': row.UnitPrice,
        'CustomerID': row.CustomerID,
        'Country': row.Country
    })

    # transformed_data = transformed_rdd.collect()
    final_df = spark.createDataFrame(transformed_rdd)

    print("data from batch_layer", final_df )

    return final_df

def clean_and_transform_data(df):
    """
    Clean and process the data.
    Args:
        df: Original DataFrame.

    Returns:
        Processed DataFrame.
    """
    # Remove null values
    df = df.dropna(subset=["InvoiceDate", "Quantity", "UnitPrice"])


    # Clean and configure columns
    df = df.withColumn("InvoiceDate", regexp_replace(col("InvoiceDate"), "[^0-9\-: ]", ""))
    df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))

    df = df \
        .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
        .withColumn("UnitPrice", col("UnitPrice").cast(FloatType())) \
        .withColumn("hour", hour(col("InvoiceDate"))) \
        .withColumn("dayofweek", dayofweek(col("InvoiceDate"))) \
        .withColumn("weekend", when(dayofweek(col("InvoiceDate")).isin(6, 7), 1).otherwise(0)) \
        .withColumn("Revenue", col("Quantity") * col("UnitPrice")) \
        .withColumn("InvoiceDate", unix_timestamp(col("InvoiceDate")).cast("double"))

    return df

def clean_and_transform_data_v2(df):
    """
    Clean and process the data, excluding rows with duplicate CustomerID.
    Args:
        df: Original DataFrame.

    Returns:
        Processed DataFrame without duplicate CustomerID rows.
    """
    # Remove null values
    df = df.dropna(subset=["InvoiceDate", "Quantity", "UnitPrice"])

    # Clean and configure columns
    df = df.withColumn("InvoiceDate", regexp_replace(F.col("InvoiceDate"), "[^0-9\-: ]", ""))
    df = df.withColumn("InvoiceDate", to_timestamp(F.col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"))

    df = df \
        .withColumn("Quantity", F.col("Quantity").cast(IntegerType())) \
        .withColumn("UnitPrice", F.col("UnitPrice").cast(FloatType())) \
        .withColumn("hour", hour(F.col("InvoiceDate"))) \
        .withColumn("dayofweek", dayofweek(F.col("InvoiceDate"))) \
        .withColumn("weekend", when(dayofweek(F.col("InvoiceDate")).isin(6, 7), 1).otherwise(0)) \
        .withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice")) \
        .withColumn("InvoiceDate", unix_timestamp(F.col("InvoiceDate")).cast("double"))

    # Remove rows with the same CustomerID, keeping only the first occurrence
    df = df.dropDuplicates(subset=["CustomerID"])

    return df


def transform_kafka_data_to_dataframe(kafka_data):
    """
    Transforms nested Kafka JSON data into a flattened Spark DataFrame.

    Args:
        kafka_data (DataFrame): Spark DataFrame containing Kafka JSON data.

    Returns:
        DataFrame: Flattened Spark DataFrame with rows per item.
    """
    # Define schema for Kafka JSON data
    item_schema = StructType([
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", DoubleType(), True)
    ])

    main_schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Items", ArrayType(item_schema), True)
    ])

    # Parse JSON and flatten the structure
    # json_df = kafka_data.selectExpr("CAST(value AS STRING) as message") \
    #     .select(from_json(col("message"), main_schema).alias("data"))

    flattened_df = kafka_data.select(
        col("InvoiceNo"),
        col("InvoiceDate"),
        col("CustomerID"),
        col("Country"),
        explode(col("Items")).alias("Item")
    ).select(
        col("InvoiceNo"),
        col("InvoiceDate"),
        col("CustomerID"),
        col("Country"),
        col("Item.StockCode").alias("StockCode"),
        col("Item.Description").alias("Description"),
        col("Item.Quantity").alias("Quantity"),
        col("Item.UnitPrice").alias("UnitPrice")
    )

    print("data in dataframe", flattened_df)

    return flattened_df

def convert_to_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return date_str  # If already in datetime.date format or invalid, return it

def get_features_spark_to_pandas(data, feature_percentage=0.8):
    """
    Extracts features for a predictive model and splits the data into 
    training and testing datasets based on a given percentage.

    Parameters:
    - data: Spark DataFrame containing the raw dataset.
    - feature_percentage: Percentage of the data to be used for training (default 80%).

    Returns:
    - X_train, y_train: Features and target variables for the training set.
    - X_test, y_test: Features and target variables for the testing set.
    """
    
    # Convert 'InvoiceDate' to date type
    data = data.withColumn('InvoiceDate', F.to_date('InvoiceDate', 'yyyy-MM-dd'))

    # Total revenue
    total_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('total_revenue'))

    # Recency (max - min InvoiceDate)
    recency = data.groupBy('CustomerID').agg((F.datediff(F.max('InvoiceDate'), F.min('InvoiceDate'))).alias('recency'))
    recency = recency.withColumn('recency', recency['recency'].cast(IntegerType()))

    # Frequency (number of invoices)
    frequency = data.groupBy('CustomerID').agg(F.count('InvoiceNo').alias('frequency'))

    # Time since first transaction
    t = data.groupBy('CustomerID').agg((F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min('InvoiceDate'))).alias('t'))

    # Time between purchases
    time_between = t.join(frequency, 'CustomerID').withColumn('time_between', (t['t'] / frequency['frequency']))

    # Average basket value
    avg_basket_value = total_rev.join(frequency, 'CustomerID').withColumn(
        'avg_basket_value', total_rev['total_revenue'] / frequency['frequency']
    )

    # Average basket size (Quantity per Invoice)
    avg_basket_size = data.groupBy('CustomerID').agg((F.sum('Quantity') / F.count('InvoiceNo')).alias('avg_basket_size'))

    # Returns (negative revenue invoices)
    returns = data.filter(data['Revenue'] < 0).groupBy('CustomerID').agg(F.count('InvoiceNo').alias('num_returns'))

    # Median purchase hour
    hour = data.groupBy('CustomerID').agg(F.percentile_approx('hour', 0.5).alias('purchase_hour_med'))

    # Median purchase day of the week
    dow = data.groupBy('CustomerID').agg(F.percentile_approx('dayofweek', 0.5).alias('purchase_dow_med'))

    # Proportion of purchases made on weekends
    weekend = data.groupBy('CustomerID').agg(F.avg('weekend').alias('purchase_weekend_prop'))

    # Combine all features into one DataFrame, ensuring unique column names
    feature_data = total_rev \
        .join(recency, 'CustomerID', 'left') \
        .join(frequency, 'CustomerID', 'left') \
        .join(t, 'CustomerID', 'left') \
        .join(time_between.select('CustomerID', 'time_between'), 'CustomerID', 'left') \
        .join(avg_basket_value.select('CustomerID', 'avg_basket_value'), 'CustomerID', 'left') \
        .join(avg_basket_size, 'CustomerID', 'left') \
        .join(returns, 'CustomerID', 'left') \
        .join(hour, 'CustomerID', 'left') \
        .join(dow, 'CustomerID', 'left') \
        .join(weekend, 'CustomerID', 'left')

    # Handle missing values by filling with 0
    feature_data = feature_data.na.fill(0)

    # Target data (revenue for the target period)
    target_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('target_rev'))

    # Join the target to the feature data
    final_data = feature_data.join(target_rev, 'CustomerID', 'left').na.fill(0)

    # Split data into training and testing datasets based on percentage
    train_data, test_data = final_data.randomSplit([feature_percentage, 1 - feature_percentage], seed=42)

    # Convert Spark DataFrame to pandas DataFrame for training and testing sets
    train_data_pandas = train_data.toPandas()
    test_data_pandas = test_data.toPandas()

    # Drop 'CustomerID' from both training and testing sets as it is not a feature for modeling
    train_data_pandas = train_data_pandas.drop('CustomerID', axis=1)
    test_data_pandas = test_data_pandas.drop('CustomerID', axis=1)

    # Split features (X) and target (y) for both training and testing sets
    X_train = train_data_pandas.drop('target_rev', axis=1)  # Features for training
    y_train = train_data_pandas['target_rev']  # Target for training
    X_test = test_data_pandas.drop('target_rev', axis=1)  # Features for testing
    y_test = test_data_pandas['target_rev']  # Target for testing

    # Return the training and testing datasets
    return X_train, y_train, X_test, y_test

# def process_streaming_features(data_stream, feature_window_days=30):
#     # Chuẩn bị dữ liệu
#     data_stream = data_stream.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
#     data_stream = data_stream.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
#     data_stream = data_stream.withColumn("dayofweek", F.dayofweek("InvoiceDate"))
#     data_stream = data_stream.withColumn("hour", F.hour("InvoiceDate"))
#     data_stream = data_stream.withColumn("weekend", (F.dayofweek("InvoiceDate") >= 6).cast(IntegerType()))

#     # Tạo cột is_return (giả sử Quantity âm có nghĩa là trả hàng)
#     data_stream = data_stream.withColumn("is_return", F.when(F.col("Quantity") < 0, True).otherwise(False).cast(BooleanType()))

#     # Định nghĩa cửa sổ tính toán (window) theo ngày
#     window_spec = F.window("InvoiceDate", f"{feature_window_days} days")
#     data_stream = data_stream.withColumn("window", window_spec)

#     # Tổng hợp dữ liệu - kết hợp nhiều phép tính
#     features = data_stream.groupBy("CustomerID", "window").agg(
#         F.sum("Revenue").alias("total_revenue"),
#         F.count("InvoiceNo").alias("frequency"),
#         F.datediff(F.current_date(), F.max("InvoiceDate")).alias("recency"),
#         F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min("InvoiceDate")).alias("t"),
#         F.sum("Quantity").alias("total_quantity"),
#         F.sum(F.when(F.col("is_return"), 1).otherwise(0)).alias("num_returns"), # Đếm số lần trả hàng
#         F.avg("weekend").alias("purchase_weekend_prop"),
#         F.collect_list("hour").alias("hours"),
#         F.collect_list("dayofweek").alias("days")
#     )

#     # Làm phẳng mảng 'hours' để tính phân vị
#     exploded_hours = F.explode("hours")  # Tạo cột exploded_hours từ hours

#     # Tính toán thêm các đặc trưng
#     features = features.withColumn("avg_basket_size", F.col("total_quantity") / F.col("frequency")).\
#         withColumn("avg_basket_value", F.col("total_revenue") / F.col("frequency")).\
#         withColumn("time_between", F.col("t") / F.col("frequency")).\
#         withColumn("purchase_hour_med", F.expr("percentile_approx(exploded_hours, 0.5)")).\
#         withColumn("purchase_dow_med", F.expr("percentile_approx(days, 0.5)")).\
#         drop("hours", "days", "t", "total_quantity", "window")

#     return features.select(
#         "CustomerID",
#         "total_revenue",
#         "recency",
#         "frequency",
#         "time_between",
#         "avg_basket_value",
#         "avg_basket_size",
#         "num_returns",
#         "purchase_hour_med",
#         "purchase_dow_med",
#         "purchase_weekend_prop"
#     )

# def process_streaming_features(data):
#     """
#     Extracts features from a Spark DataFrame in real-time, without splitting into
#     training and testing datasets. This function processes the streaming data and
#     returns the features.

#     Parameters:
#     - data: Spark DataFrame containing the raw streaming dataset.

#     Returns:
#     - features: Spark DataFrame with extracted features for real-time processing.
#     """

#     print("process_streaming_features", data.show())

#     # data = data.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))


#     # Convert 'InvoiceDate' to date type
#     # data = data.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
#     data = data.withColumn('InvoiceDate', F.to_date('InvoiceDate', 'yyyy-MM-dd'))

#     # Total revenue
#     total_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('total_revenue'))

#     # Recency (max - min InvoiceDate)
#     recency = data.groupBy('CustomerID').agg((F.datediff(F.max('InvoiceDate'), F.min('InvoiceDate'))).alias('recency'))
#     recency = recency.withColumn('recency', recency['recency'].cast(IntegerType()))

#     # Frequency (number of invoices)
#     frequency = data.groupBy('CustomerID').agg(F.count('InvoiceNo').alias('frequency'))

#     # Time since first transaction
#     # t = data.groupBy('CustomerID').agg((F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min('InvoiceDate'))).alias('t'))
#     # Tạo ngày cố định trong UTC (2011-06-11)
#     fixed_date = datetime(2011, 6, 11).replace(tzinfo=None)  # Không có múi giờ, mặc định là UTC

#     # Tính toán sự khác biệt giữa ngày trong 'InvoiceDate' và ngày cố định
#     t = data.groupBy('CustomerID').agg(
#         (F.datediff(F.lit(fixed_date), F.min('InvoiceDate'))).alias('t')
#     )


#     # Time between purchases
#     time_between = t.join(frequency, 'CustomerID').withColumn('time_between', (t['t'] / frequency['frequency']))

#     # Average basket value
#     avg_basket_value = total_rev.join(frequency, 'CustomerID').withColumn(
#         'avg_basket_value', total_rev['total_revenue'] / frequency['frequency']
#     )

#     # Average basket size (Quantity per Invoice)
#     avg_basket_size = data.groupBy('CustomerID').agg((F.sum('Quantity') / F.count('InvoiceNo')).alias('avg_basket_size'))

#     # Returns (negative revenue invoices)
#     returns = data.filter(data['Revenue'] < 0).groupBy('CustomerID').agg(F.count('InvoiceNo').alias('num_returns'))

#     # Median purchase hour
#     hour = data.groupBy('CustomerID').agg(F.percentile_approx('hour', 0.5).alias('purchase_hour_med'))

#     # Median purchase day of the week
#     dow = data.groupBy('CustomerID').agg(F.percentile_approx('dayofweek', 0.5).alias('purchase_dow_med'))

#     # Proportion of purchases made on weekends
#     weekend = data.groupBy('CustomerID').agg(F.avg('weekend').alias('purchase_weekend_prop'))

#     # Combine all features into one DataFrame, ensuring unique column names
#     feature_data = total_rev \
#         .join(recency, 'CustomerID', 'left') \
#         .join(frequency, 'CustomerID', 'left') \
#         .join(t, 'CustomerID', 'left') \
#         .join(time_between.select('CustomerID', 'time_between'), 'CustomerID', 'left') \
#         .join(avg_basket_value.select('CustomerID', 'avg_basket_value'), 'CustomerID', 'left') \
#         .join(avg_basket_size, 'CustomerID', 'left') \
#         .join(returns, 'CustomerID', 'left') \
#         .join(hour, 'CustomerID', 'left') \
#         .join(dow, 'CustomerID', 'left') \
#         .join(weekend, 'CustomerID', 'left')

#     # Handle missing values by filling with 0
#     feature_data = feature_data.na.fill(0)

#     # Join the target to the feature data (revenue for the target period)
#     target_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('target_rev'))
#     final_data = feature_data.join(target_rev, 'CustomerID', 'left').na.fill(0)

#     # Remove 'CustomerID' as it is not a feature for modeling
#     final_data = final_data.drop('CustomerID', 'target_rev')


#     # Return the processed feature data for real-time prediction
#     return final_data

def process_streaming_features(data):
    """
    Extracts features from a Spark DataFrame in real-time, without splitting into
    training and testing datasets. This function processes the streaming data and
    returns the features, grouped by CustomerID.

    Parameters:
    - data: Spark DataFrame containing the raw streaming dataset.

    Returns:
    - features: Spark DataFrame with extracted features for real-time processing.
    """

    # Convert 'InvoiceDate' to date type
    data = data.withColumn('InvoiceDate', F.to_date('InvoiceDate', 'yyyy-MM-dd'))

    # Total revenue
    total_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('total_revenue'))

    # Recency (max - min InvoiceDate)
    recency = data.groupBy('CustomerID').agg((F.datediff(F.max('InvoiceDate'), F.min('InvoiceDate'))).alias('recency'))
    recency = recency.withColumn('recency', recency['recency'].cast(IntegerType()))

    # Frequency (number of invoices)
    frequency = data.groupBy('CustomerID').agg(F.count('InvoiceNo').alias('frequency'))

    # Time since first transaction
    fixed_date = datetime(2011, 6, 11).replace(tzinfo=None)
    t = data.groupBy('CustomerID').agg(
        (F.datediff(F.lit(fixed_date), F.min('InvoiceDate'))).alias('t')
    )

    # Time between purchases
    time_between = t.join(frequency, 'CustomerID').withColumn('time_between', (t['t'] / frequency['frequency']))

    # Average basket value
    avg_basket_value = total_rev.join(frequency, 'CustomerID').withColumn(
        'avg_basket_value', total_rev['total_revenue'] / frequency['frequency']
    )

    # Average basket size (Quantity per Invoice)
    avg_basket_size = data.groupBy('CustomerID').agg((F.sum('Quantity') / F.count('InvoiceNo')).alias('avg_basket_size'))

    # Returns (negative revenue invoices)
    returns = data.filter(data['Revenue'] < 0).groupBy('CustomerID').agg(F.count('InvoiceNo').alias('num_returns'))

    # Median purchase hour
    hour = data.groupBy('CustomerID').agg(F.percentile_approx('hour', 0.5).alias('purchase_hour_med'))

    # Median purchase day of the week
    dow = data.groupBy('CustomerID').agg(F.percentile_approx('dayofweek', 0.5).alias('purchase_dow_med'))

    # Proportion of purchases made on weekends
    weekend = data.groupBy('CustomerID').agg(F.avg('weekend').alias('purchase_weekend_prop'))

    # Combine all features into one DataFrame, ensuring unique column names
    feature_data = total_rev \
        .join(recency, 'CustomerID', 'left') \
        .join(frequency, 'CustomerID', 'left') \
        .join(t, 'CustomerID', 'left') \
        .join(time_between.select('CustomerID', 'time_between'), 'CustomerID', 'left') \
        .join(avg_basket_value.select('CustomerID', 'avg_basket_value'), 'CustomerID', 'left') \
        .join(avg_basket_size, 'CustomerID', 'left') \
        .join(returns, 'CustomerID', 'left') \
        .join(hour, 'CustomerID', 'left') \
        .join(dow, 'CustomerID', 'left') \
        .join(weekend, 'CustomerID', 'left')

    # Handle missing values by filling with 0
    feature_data = feature_data.na.fill(0)

    # Join the target to the feature data (revenue for the target period)
    target_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('target_rev'))
    final_data = feature_data.join(target_rev, 'CustomerID', 'left').na.fill(0)

    # Remove 'CustomerID' as it is not a feature for modeling
    final_data = final_data.drop('CustomerID', 'target_rev')

    # Return the processed feature data for real-time prediction
    return final_data


def spark_processing(spark):
    """
    Main program for data processing.
    """
    # Create SparkSession
    # spark = spark

    # File paths for input/output in HDFS
    input_path = "hdfs://localhost:9000/batch-layer/raw_data.csv"
    output_path = "hdfs://localhost:9000/path/to/processed_data.csv"

    # Read data from HDFS
    print("Reading data from HDFS...")
    df = read_data_from_hdfs(spark, input_path)

    # Clean and process the data
    print("Cleaning and processing the data...")
    processed_df = clean_and_transform_data(df)

    # Display results
    print("Processed data:")
    processed_df.show(10)

    # Save the processed data to HDFS
    print("Saving the processed data to HDFS...")
    # save_data_to_hdfs(processed_df, output_path)
    save_data_to_postgresql(processed_df)

    print("Fine-tuning the model with processed data...")
    # Assuming X_train and y_train are prepared from the processed data

    X_train, y_train, X_test, y_test = get_features_spark_to_pandas(processed_df, feature_percentage=0.8)

    print("X train ", X_train.columns)
    
    # Call the function to load and fine-tune the model
    path = '/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras'
    load_and_finetune_model(path, X_train, y_train)

    print("Processing complete!")

def spark_processing_v2(spark):
    """
    Main program for data processing.
    """
    # Create SparkSession
    

    # File paths for input/output in HDFS
    input_path = "hdfs://namenode:9000/batch-layer/raw_data.json"
    # output_path = "hdfs://namenode:9000/path/to/processed_data.csv"

    # Read data from HDFS
    print("Reading data from HDFS...")

    schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Items", ArrayType(StructType([
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", FloatType(), True),
            StructField("UnitPrice", FloatType(), True)
        ])), True)
    ])

    print("before read spark", spark)
    print("Is SparkSession active:", spark.sparkContext._jsc.sc().isStopped())

    try:
        raw_df = spark.read.option("multiline","true").schema(schema).json(input_path)
        print("Data from HDFS", raw_df.show())
        print("Data successfully read from HDFS.")
    except Exception as e:
        print(f"Error during  reading data: {e}")

    
    df = read_data_from_hdfs(spark, input_path)
    batch_df =read_format_json_from_hdfs(spark, input_path)

    # print("data from spark", df.show())

    # Clean and process the data
    print("Cleaning and processing the data...")
    processed_df_before = clean_and_transform_data_v2(batch_df)
    processed_df = process_streaming_features(processed_df_before)

    print("Cleaning and processing the data...", processed_df)


    # Thực hiện dự đoán với mô hình ML
    try:
        features = processed_df.toPandas()
        # features = processed_df_before.toPandas()
        print("processed_df", features.head())


        df_with_predictions = processed_df_before

        print("Features (Pandas DataFrame) Info:")
        print(features.info())  # In thông tin chi tiết
        print("Features Head:")
        print(features.head())  # In một vài dòng đầu tiên        
        predictions = model.predict(features)

        prediction_value = float(predictions[0][0])

        # df_with_predictions["CLV_Prediction"] = predictions[0][0] 
        df_with_predictions = df_with_predictions.withColumn(
            "CLV_Prediction", lit(prediction_value)
        ) 

        columns_to_drop = ["hour", "dayofweek", "weekend"]
        df_with_predictions = df_with_predictions.drop(*columns_to_drop)

        df_with_predictions = df_with_predictions.withColumn("InvoiceDate", to_timestamp("InvoiceDate"))

        print("data with predict", df_with_predictions.show())
        print("data with predict", df_with_predictions.printSchema())


        db_url = "jdbc:postgresql://172.20.0.8:5432/airflow"

        db_properties = {
        "user": "postgres",
        "password": "123",
        "driver": "org.postgresql.Driver"
        }

        save_to_postgres(df_with_predictions, "sales_data_order", db_url, db_properties)


    except Exception as e:
        print(f"Error during model prediction: {e}")



def write_to_hbase(dataframe, table_name='hbase-clv'):
    """
    Write Spark DataFrame to HBase with custom column mappings and rowkey generation.
    """
    try:
        # Tạo một cột "rowkey" dựa trên logic trong insert_data_to_hbase
        from pyspark.sql.functions import concat_ws, lit, col

        dataframe = dataframe.withColumn(
            "rowkey", 
            concat_ws("-", 
                      col("InvoiceNo").cast("string"), 
                      col("CustomerID").cast("string"))
        )

        # Ánh xạ dữ liệu đến các column family và cột trong HBase
        catalog = f"""
        {{
            "table":{{"namespace":"default", "name":"{table_name}"}},
            "rowkey":"rowkey",
            "columns":{{
                "rowkey":{{"cf":"rowkey", "col":"key", "type":"string"}},
                "Quantity":{{"cf":"cf", "col":"Quantity", "type":"string"}},
                "UnitPrice":{{"cf":"cf", "col":"UnitPrice", "type":"string"}},
                "InvoiceDate":{{"cf":"cf", "col":"InvoiceDate", "type":"string"}},
                "hour":{{"cf":"cf", "col":"hour", "type":"string"}},
                "dayofweek":{{"cf":"cf", "col":"dayofweek", "type":"string"}},
                "weekend":{{"cf":"cf", "col":"weekend", "type":"string"}},
                "Revenue":{{"cf":"cf", "col":"Revenue", "type":"string"}},
                "CLV_Prediction":{{"cf":"cf", "col":"CLV_Prediction", "type":"string"}}
            }}
        }}
        """

        # Ghi dữ liệu vào HBase
        dataframe.write \
            .format("org.apache.hadoop.hbase.spark") \
            .option("hbase.catalog", catalog) \
            .option("hbase.table", table_name) \
            .mode("append") \
            .save()

        print(f"Successfully wrote data to HBase table: {table_name}")

    except Exception as e:
        print(f"Error writing to HBase: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    spark_processing()
