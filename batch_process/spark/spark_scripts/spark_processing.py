from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, hour, dayofweek, when, unix_timestamp
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import functions as F
from datetime import datetime

from batch_process.postgres.save_preprocessed_data import save_data_to_postgresql
from batch_process.model.model_update import load_and_finetune_model

def create_spark_session():
    """
    Create a SparkSession.
    """
    spark = SparkSession.builder \
        .appName("Data Cleaning and Transformation") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    return spark


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
#     """
#     Processes real-time streaming data to extract features for the predictive model.
#     Returns only the necessary features (11 attributes) for the model.

#     Parameters:
#     - data_stream: Spark Streaming DataFrame (structured streaming input).
#     - feature_window_days: The time window (in days) to compute features (default 30 days).

#     Returns:
#     - features: DataFrame with the extracted features for the model.
#     """

#     # Extract date parts from InvoiceDate
#     data_stream = data_stream.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
#     data_stream = data_stream.withColumn("InvoiceDate", F.to_date("InvoiceDate", "yyyy-MM-dd"))
#     data_stream = data_stream.withColumn("dayofweek", F.dayofweek("InvoiceDate"))
#     data_stream = data_stream.withColumn("hour", F.hour("InvoiceDate"))
#     data_stream = data_stream.withColumn("weekend", (F.dayofweek("InvoiceDate") >= 6).cast(IntegerType()))

#     # Define feature computation logic within the sliding window
#     window_spec = F.window("InvoiceDate", f"{feature_window_days} days")

#     # Total revenue (sum of revenue per customer in the window)
#     total_rev = data_stream.groupBy("CustomerID", window_spec).agg(F.sum("Revenue").alias("total_revenue"))

#     # Recency (days since last purchase in the window)
#     recency = data_stream.groupBy("CustomerID", window_spec).agg(
#         (F.datediff(F.current_date(), F.max("InvoiceDate"))).alias("recency")
#     )

#     # Frequency (number of transactions in the window)
#     frequency = data_stream.groupBy("CustomerID", window_spec).agg(F.count("InvoiceNo").alias("frequency"))

#     # Average basket value (total revenue per number of invoices)
#     avg_basket_value = total_rev.join(frequency, ["CustomerID", "window"]).withColumn(
#         "avg_basket_value", total_rev["total_revenue"] / frequency["frequency"]
#     )

#     # Time between purchases (average time between purchases per customer)
#     t = data_stream.groupBy("CustomerID", window_spec).agg(
#         (F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min("InvoiceDate"))).alias("t")
#     )
#     time_between = t.join(frequency, ["CustomerID", "window"]).withColumn(
#         "time_between", t["t"] / frequency["frequency"]
#     )

#     # Average basket size (total quantity per number of invoices)
#     avg_basket_size = data_stream.groupBy("CustomerID", window_spec).agg(
#         (F.sum("Quantity") / F.count("InvoiceNo")).alias("avg_basket_size")
#     )

#     # Proportion of purchases on weekends (whether the purchase was made on the weekend)
#     weekend = data_stream.groupBy("CustomerID", window_spec).agg(
#         F.avg("weekend").alias("purchase_weekend_prop")
#     )

#     # Combine all features (the 11 features we need for the model)
#     features = total_rev \
#         .join(recency, ["CustomerID", "window"], "left") \
#         .join(frequency, ["CustomerID", "window"], "left") \
#         .join(avg_basket_value, ["CustomerID", "window"], "left") \
#         .join(avg_basket_size, ["CustomerID", "window"], "left") \
#         .join(time_between.select("CustomerID", "window", "time_between"), ["CustomerID", "window"], "left") \
#         .join(weekend, ["CustomerID", "window"], "left") \
#         .na.fill(0) \
#         .drop("window")  # Drop the window column as it's not needed for modeling

#     # Return only the features (11 columns)
#     return features.toPandas()

# def process_streaming_features(data_stream, feature_window_days=30):
#     """
#         Processes real-time streaming data to extract features for the predictive model.
#         Returns only the necessary features (11 attributes) for the model.

#         Parameters:
#         - data_stream: Spark Streaming DataFrame (structured streaming input).
#         - feature_window_days: The time window (in days) to compute features (default 30 days).

#         Returns:
#         - features: DataFrame with the extracted features for the model.
#     """
#     # Trích xuất các phần từ ngày của InvoiceDate
#     data_stream = data_stream.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
#     # data_stream = data_stream.withColumn("InvoiceDate", F.to_date("InvoiceDate", "yyyy-MM-dd"))
#     data_stream = data_stream.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
#     data_stream = data_stream.withColumn("dayofweek", F.dayofweek("InvoiceDate"))
#     data_stream = data_stream.withColumn("hour", F.hour("InvoiceDate"))
#     data_stream = data_stream.withColumn("weekend", (F.dayofweek("InvoiceDate") >= 6).cast(IntegerType()))

#     # Định nghĩa logic tính toán đặc trưng trong cửa sổ trượt
#     window_spec = F.window("InvoiceDate", f"{feature_window_days} days")

#     # Thêm cột window cho mỗi DataFrame để tránh nhầm lẫn
#     data_stream = data_stream.withColumn("window", window_spec)

#     # Gán alias cho các DataFrame để tránh nhầm lẫn khi join
#     total_rev = data_stream.groupBy("CustomerID", "window").agg(F.sum("Revenue").alias("total_revenue")).alias("total_rev")
#     recency = data_stream.groupBy("CustomerID", "window").agg(
#         (F.datediff(F.current_date(), F.max("InvoiceDate"))).alias("recency")).alias("recency")
#     frequency = data_stream.groupBy("CustomerID", "window").agg(F.count("InvoiceNo").alias("frequency")).alias("frequency")
#     avg_basket_value = total_rev.join(frequency, ["CustomerID", "window"]).withColumn(
#         "avg_basket_value", total_rev["total_revenue"] / frequency["frequency"]).alias("avg_basket_value")
#     t = data_stream.groupBy("CustomerID", "window").agg(
#         (F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min("InvoiceDate"))).alias("t")).alias("t")
#     time_between = t.join(frequency, ["CustomerID", "window"]).withColumn(
#         "time_between", t["t"] / frequency["frequency"]).alias("time_between")
#     avg_basket_size = data_stream.groupBy("CustomerID", "window").agg(
#         (F.sum("Quantity") / F.count("InvoiceNo")).alias("avg_basket_size")).alias("avg_basket_size")
#     weekend = data_stream.groupBy("CustomerID", "window").agg(
#         F.avg("weekend").alias("purchase_weekend_prop")).alias("weekend")

#     # Kết hợp tất cả các đặc trưng (11 đặc trưng cần thiết cho mô hình)
#     features = total_rev \
#         .join(recency, ["CustomerID", "window"], "left") \
#         .join(frequency, ["CustomerID", "window"], "left") \
#         .join(avg_basket_value, ["CustomerID", "window"], "left") \
#         .join(avg_basket_size, ["CustomerID", "window"], "left") \
#         .join(time_between.select("CustomerID", "window", "time_between"), ["CustomerID", "window"], "left") \
#         .join(weekend, ["CustomerID", "window"], "left") \
#         .na.fill(0) \
#         .drop("window")  # Loại bỏ cột window vì không cần thiết cho mô hình

#     # Trả về chỉ các đặc trưng (11 cột)
#     return features.toPandas()

# def process_streaming_features(data_stream, feature_window_days=30):
#     """
#     Processes real-time streaming data to extract features for the predictive model.
#     Returns only the necessary features (11 attributes) for the model.

#     Parameters:
#     - data_stream: Spark Streaming DataFrame (structured streaming input).
#     - feature_window_days: The time window (in days) to compute features (default 30 days).

#     Returns:
#     - features: DataFrame with the extracted features for the model.
#     """
#     # Trích xuất các phần từ ngày của InvoiceDate
#     data_stream = data_stream.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
#     data_stream = data_stream.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
#     data_stream = data_stream.withColumn("dayofweek", F.dayofweek("InvoiceDate"))
#     data_stream = data_stream.withColumn("hour", F.hour("InvoiceDate"))
#     data_stream = data_stream.withColumn("weekend", (F.dayofweek("InvoiceDate") >= 6).cast(IntegerType()))

#     # Định nghĩa logic tính toán đặc trưng trong cửa sổ trượt
#     window_spec = F.window("InvoiceDate", f"{feature_window_days} days")

#     # Thêm cột window cho mỗi DataFrame để tránh nhầm lẫn
#     data_stream = data_stream.withColumn("window", window_spec)

#     # Gán alias cho các DataFrame để tránh nhầm lẫn khi join
#     total_rev = data_stream.groupBy("CustomerID", "window").agg(F.sum("Revenue").alias("total_revenue")).alias("total_rev")
#     recency = data_stream.groupBy("CustomerID", "window").agg(
#         (F.datediff(F.current_date(), F.max("InvoiceDate"))).alias("recency")).alias("recency")
    
#     # Sử dụng alias rõ ràng cho frequency
#     frequency = data_stream.groupBy("CustomerID", "window").agg(F.count("InvoiceNo").alias("frequency")).alias("frequency")
    
#     # Thêm alias cho cột tổng doanh thu để tránh mơ hồ khi join
#     avg_basket_value = total_rev.join(frequency, ["CustomerID", "window"], "left").withColumn(
#         "avg_basket_value", total_rev["total_revenue"] / frequency["frequency"]).alias("avg_basket_value")

#     t = data_stream.groupBy("CustomerID", "window").agg(
#         (F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min("InvoiceDate"))).alias("t")).alias("t")

#     time_between = t.join(frequency, ["CustomerID", "window"]).withColumn(
#         "time_between", t["t"] / frequency["frequency"]).alias("time_between")

#     avg_basket_size = data_stream.groupBy("CustomerID", "window").agg(
#         (F.sum("Quantity") / F.count("InvoiceNo")).alias("avg_basket_size")).alias("avg_basket_size")

#     weekend = data_stream.groupBy("CustomerID", "window").agg(
#         F.avg("weekend").alias("purchase_weekend_prop")).alias("weekend")

#     # Kết hợp tất cả các đặc trưng (11 đặc trưng cần thiết cho mô hình)
#     features = total_rev \
#         .join(recency, ["CustomerID", "window"], "left") \
#         .join(frequency, ["CustomerID", "window"], "left") \
#         .join(avg_basket_value, ["CustomerID", "window"], "left") \
#         .join(avg_basket_size, ["CustomerID", "window"], "left") \
#         .join(time_between.select("CustomerID", "window", "time_between"), ["CustomerID", "window"], "left") \
#         .join(weekend, ["CustomerID", "window"], "left") \
#         .na.fill(0) \
#         .drop("window")  # Loại bỏ cột window vì không cần thiết cho mô hình

#     # Chọn các cột với tên duy nhất (tránh mơ hồ)
#     return features.select(
#         "CustomerID", 
#         "total_rev.total_revenue",  # Sử dụng alias cho total_revenue
#         "recency", 
#         "frequency.frequency",  # Sử dụng alias cho frequency
#         "avg_basket_value", 
#         "time_between", 
#         "avg_basket_size", 
#         "num_returns", 
#         "purchase_hour_med", 
#         "purchase_dow_med", 
#         "purchase_weekend_prop"
#     )


# def process_streaming_features(data_stream, feature_window_days=30):
#     """
#     Processes real-time streaming data to extract features for the predictive model.
#     Returns only the necessary features (11 attributes) for the model.

#     Parameters:
#     - data_stream: Spark Streaming DataFrame (structured streaming input).
#     - feature_window_days: The time window (in days) to compute features (default 30 days).

#     Returns:
#     - features: DataFrame with the extracted features for the model.
#     """
#     # Trích xuất các phần từ ngày của InvoiceDate
#     data_stream = data_stream.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
#     data_stream = data_stream.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
#     data_stream = data_stream.withColumn("dayofweek", F.dayofweek("InvoiceDate"))
#     data_stream = data_stream.withColumn("hour", F.hour("InvoiceDate"))
#     data_stream = data_stream.withColumn("weekend", (F.dayofweek("InvoiceDate") >= 6).cast(IntegerType()))

#     # Định nghĩa logic tính toán đặc trưng trong cửa sổ trượt
#     window_spec = F.window("InvoiceDate", f"{feature_window_days} days")

#     # Thêm cột window cho mỗi DataFrame để tránh nhầm lẫn
#     data_stream = data_stream.withColumn("window", window_spec)

#     # Gán alias cho các DataFrame để tránh nhầm lẫn khi join
#     total_rev = data_stream.groupBy("CustomerID", "window").agg(F.sum("Revenue").alias("total_revenue"))
#     recency = data_stream.groupBy("CustomerID", "window").agg(
#         (F.datediff(F.current_date(), F.max("InvoiceDate"))).alias("recency"))
    
#     frequency = data_stream.groupBy("CustomerID", "window").agg(F.count("InvoiceNo").alias("frequency"))
    
#     avg_basket_value = total_rev.join(frequency, ["CustomerID", "window"], "left").withColumn(
#         "avg_basket_value", total_rev["total_revenue"] / frequency["frequency"])

#     t = data_stream.groupBy("CustomerID", "window").agg(
#         (F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min("InvoiceDate"))).alias("t"))

#     time_between = t.join(frequency, ["CustomerID", "window"]).withColumn(
#         "time_between", t["t"] / frequency["frequency"])

#     avg_basket_size = data_stream.groupBy("CustomerID", "window").agg(
#         (F.sum("Quantity") / F.count("InvoiceNo")).alias("avg_basket_size"))

#     weekend = data_stream.groupBy("CustomerID", "window").agg(
#         F.avg("weekend").alias("purchase_weekend_prop"))

#     # Kết hợp tất cả các đặc trưng (11 đặc trưng cần thiết cho mô hình)
#     features = total_rev \
#         .join(recency, ["CustomerID", "window"], "left") \
#         .join(frequency, ["CustomerID", "window"], "left") \
#         .join(avg_basket_value, ["CustomerID", "window"], "left") \
#         .join(avg_basket_size, ["CustomerID", "window"], "left") \
#         .join(time_between.select("CustomerID", "window", "time_between"), ["CustomerID", "window"], "left") \
#         .join(weekend, ["CustomerID", "window"], "left") \
#         .na.fill(0) \
#         .drop("window")  # Loại bỏ cột window vì không cần thiết cho mô hình

#     # Sửa select với alias rõ ràng
#     return features.select(
#         "CustomerID", 
#         "total_revenue",  # Đảm bảo không cần alias cho total_revenue nữa
#         "recency",  # Đảm bảo không cần alias cho recency nữa
#         "frequency",  # Đảm bảo không cần alias cho frequency nữa
#         "avg_basket_value", 
#         "time_between", 
#         "avg_basket_size", 
#         "purchase_hour_med", 
#         "purchase_dow_med", 
#         "purchase_weekend_prop"
#     )

def process_streaming_features(data_stream, feature_window_days=30):
    # Data preparation (add is_return column if necessary)
    data_stream = data_stream.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))
    data_stream = data_stream.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
    data_stream = data_stream.withColumn("dayofweek", F.dayofweek("InvoiceDate"))
    data_stream = data_stream.withColumn("hour", F.hour("InvoiceDate"))
    data_stream = data_stream.withColumn("weekend", (F.dayofweek("InvoiceDate") >= 6).cast(IntegerType()))
    # Assuming 'is_return' column exists or you add logic to create it here based on your data.
    # Example:  data_stream = data_stream.withColumn("is_return", F.when(F.col("Quantity") < 0, True).otherwise(False).cast(BooleanType()))

    window_spec = F.window("InvoiceDate", f"{feature_window_days} days")
    data_stream = data_stream.withColumn("window", window_spec)

    # Aggregation - combined for efficiency
    features = data_stream.groupBy("CustomerID", "window").agg(
        F.sum("Revenue").alias("total_revenue"),
        F.count("InvoiceNo").alias("frequency"),
        F.datediff(F.current_date(), F.max("InvoiceDate")).alias("recency"),
        F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min("InvoiceDate")).alias("t"),
        F.sum("Quantity").alias("total_quantity"),
        F.sum(F.when(F.col("is_return"), 1).otherwise(0)).alias("num_returns"), # Count returns
        F.avg("weekend").alias("purchase_weekend_prop"),
        F.collect_list("hour").alias("hours"),
        F.collect_list("dayofweek").alias("days")
    )


    # Calculate derived features
    features = features.withColumn("avg_basket_size", F.col("total_quantity") / F.col("frequency")).\
        withColumn("avg_basket_value", F.col("total_revenue") / F.col("frequency")).\
        withColumn("time_between", F.col("t") / F.col("frequency")).\
        withColumn("purchase_hour_med", F.expr("percentile_approx(hours, 0.5)")).\
        withColumn("purchase_dow_med", F.expr("percentile_approx(days, 0.5)")).\
        drop("hours", "days", "t", "total_quantity", "window")


    return features.select(
        "CustomerID",
        "total_revenue",
        "recency",
        "frequency",
        "time_between",
        "avg_basket_value",
        "avg_basket_size",
        "num_returns",
        "purchase_hour_med",
        "purchase_dow_med",
        "purchase_weekend_prop"
    )

def spark_processing(spark):
    """
    Main program for data processing.
    """
    # Create SparkSession
    spark = spark

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


if __name__ == "__main__":
    spark_processing()
