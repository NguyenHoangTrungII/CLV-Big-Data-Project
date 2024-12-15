from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, hour, dayofweek, when, unix_timestamp
from pyspark.sql.types import BooleanType,IntegerType, FloatType
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

def process_streaming_features(data):
    """
    Extracts features from a Spark DataFrame in real-time, without splitting into
    training and testing datasets. This function processes the streaming data and
    returns the features.

    Parameters:
    - data: Spark DataFrame containing the raw streaming dataset.

    Returns:
    - features: Spark DataFrame with extracted features for real-time processing.
    """

    # data = data.withColumn("Revenue", F.col("Quantity") * F.col("UnitPrice"))


    # Convert 'InvoiceDate' to date type
    # data = data.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
    data = data.withColumn('InvoiceDate', F.to_date('InvoiceDate', 'yyyy-MM-dd'))

    # Total revenue
    total_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('total_revenue'))

    # Recency (max - min InvoiceDate)
    recency = data.groupBy('CustomerID').agg((F.datediff(F.max('InvoiceDate'), F.min('InvoiceDate'))).alias('recency'))
    recency = recency.withColumn('recency', recency['recency'].cast(IntegerType()))

    # Frequency (number of invoices)
    frequency = data.groupBy('CustomerID').agg(F.count('InvoiceNo').alias('frequency'))

    # Time since first transaction
    # t = data.groupBy('CustomerID').agg((F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min('InvoiceDate'))).alias('t'))
    # Tạo ngày cố định trong UTC (2011-06-11)
    fixed_date = datetime(2011, 6, 11).replace(tzinfo=None)  # Không có múi giờ, mặc định là UTC

    # Tính toán sự khác biệt giữa ngày trong 'InvoiceDate' và ngày cố định
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
