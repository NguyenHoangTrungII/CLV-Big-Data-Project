# import pandas as pd
# from hdfs import InsecureClient
# from io import BytesIO

# # Step 1: Connect to HDFS and read the data
# def read_data_from_hdfs():
#     hdfs_host = 'localhost'
#     hdfs_port = 50070
#     hdfs_user = 'nhtrung'
#     file_path = '/batch-layer/raw_data.csv'

#     client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user=hdfs_user)
    
#     try:
#         # Read data from HDFS into a DataFrame
#         with client.read(file_path) as reader:
#             df = pd.read_csv(reader)
#         print("Data loaded from HDFS successfully.")
#         return df
#     except Exception as e:
#         print(f"Error reading data from HDFS: {e}")
#         return pd.DataFrame()

# # Step 2: Data Cleaning and Preprocessing
# def clean_and_process_data():

#     df = read_data_from_hdfs()

#     if df.empty:
#         print("No data to process.")
#         return df

#     # Convert 'InvoiceDate' to datetime format
#     df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')

#     # Drop rows where CustomerID is null
#     df_cleaned = df.dropna(subset=['CustomerID'])
#     df_cleaned['CustomerID'] = df_cleaned['CustomerID'].astype(str)

#     # Remove rows with non-positive quantities and prices
#     df_cleaned = df_cleaned[(df_cleaned['Quantity'] > 0) & (df_cleaned['UnitPrice'] > 0)]

#     # Display the cleaned data summary
#     print(f"Remaining rows after cleaning: {df_cleaned.shape[0]}")
#     print("Summary Statistics:")
#     print(df_cleaned.describe())

#     return df_cleaned


# # Main function
# def main():
#     print("Starting data processing...")
#     cleaned_data = clean_and_process_data()
    
#     if not cleaned_data.empty:
#         print("Data processing completed successfully.")
#     else:
#         print("Data processing failed or no data to process.")

# # Run the main function if the script is executed directly
# if __name__ == "__main__":
#     main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, hour, dayofweek, when, unix_timestamp
from pyspark.sql.types import IntegerType, FloatType

from batch_process.postgres.save_preprocessed_data import save_data_to_postgresql
from batch_process.model.model_update import load_and_finetune_model


from pyspark.sql import functions as F
from datetime import datetime



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

# def get_features_spark_to_pandas(data, feature_start, feature_end, target_start, target_end):
#     # Ensure proper date formatting
#     if type(feature_start) != datetime.date:
#         feature_start = datetime.strptime(feature_start, '%Y-%m-%d').date()
#     if type(feature_end) != datetime.date:
#         feature_end = datetime.strptime(feature_end, '%Y-%m-%d').date()
#     if type(target_start) != datetime.date:
#         target_start = datetime.strptime(target_start, '%Y-%m-%d').date()
#     if type(target_end) != datetime.date:
#         target_end = datetime.strptime(target_end, '%Y-%m-%d').date()

#     # Filter data for feature period
#     features_data = data.filter((data.date >= F.lit(feature_start)) & (data.date <= F.lit(feature_end)))
#     print(f'Using data from {(feature_end - feature_start).days} days')
#     print(f'To predict {(target_end - target_start).days} days')

#     # Transactions data features
#     total_rev = features_data.groupBy('CustomerID').agg(F.sum('Revenue').alias('total_revenue'))
#     recency = features_data.groupBy('CustomerID').agg((F.max('date') - F.min('date')).alias('recency'))
#     recency = recency.withColumn('recency', recency['recency'].cast(IntegerType()))
#     frequency = features_data.groupBy('CustomerID').agg(F.count('InvoiceNo').alias('frequency'))
#     t = features_data.groupBy('CustomerID').agg((F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min('date'))).alias('t'))
#     time_between = t.join(frequency, 'CustomerID').withColumn('time_between', (t['t'] / frequency['frequency']))
#     avg_basket_value = total_rev.join(frequency, 'CustomerID').withColumn('avg_basket_value', total_rev['total_revenue'] / frequency['frequency'])
#     avg_basket_size = features_data.groupBy('CustomerID').agg((F.sum('Quantity') / F.count('InvoiceNo')).alias('avg_basket_size'))
#     returns = features_data.filter(features_data['Revenue'] < 0).groupBy('CustomerID').agg(F.count('InvoiceNo').alias('num_returns'))
#     hour = features_data.groupBy('CustomerID').agg(F.percentile_approx('hour', 0.5).alias('purchase_hour_med'))
#     dow = features_data.groupBy('CustomerID').agg(F.percentile_approx('dayofweek', 0.5).alias('purchase_dow_med'))
#     weekend = features_data.groupBy('CustomerID').agg(F.avg('weekend').alias('purchase_weekend_prop'))

#     # Combine all features into one DataFrame
#     feature_data = total_rev.join(recency, 'CustomerID', 'left') \
#                             .join(frequency, 'CustomerID', 'left') \
#                             .join(t, 'CustomerID', 'left') \
#                             .join(time_between, 'CustomerID', 'left') \
#                             .join(avg_basket_value, 'CustomerID', 'left') \
#                             .join(avg_basket_size, 'CustomerID', 'left') \
#                             .join(returns, 'CustomerID', 'left') \
#                             .join(hour, 'CustomerID', 'left') \
#                             .join(dow, 'CustomerID', 'left') \
#                             .join(weekend, 'CustomerID', 'left')

#     # Handle missing values by filling with 0
#     feature_data = feature_data.na.fill(0)

#     # Target data
#     target_data = data.filter((data.date >= F.lit(target_start)) & (data.date <= F.lit(target_end)))
#     target_rev = target_data.groupBy('CustomerID').agg(F.sum('Revenue').alias('target_rev'))

#     # Join the target to the feature data
#     final_data = feature_data.join(target_rev, 'CustomerID', 'left').na.fill(0)

#     # Convert Spark DataFrame to pandas DataFrame
#     final_data_pandas = final_data.toPandas()

#     # Return features (X) and target (y) as pandas DataFrames
#     X = final_data_pandas.drop('target_rev', axis=1)  # Features DataFrame
#     y = final_data_pandas['target_rev']  # Target Series

#     return X, y


from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FeatureExtraction").getOrCreate()

# def get_features_spark_to_pandas(data, feature_percentage=0.8):
#     """
#     Extracts features for a predictive model and splits the data into 
#     training and testing datasets based on a given percentage.

#     Parameters:
#     - data: Spark DataFrame containing the raw dataset.
#     - feature_percentage: Percentage of the data to be used for training (default 80%).

#     Returns:
#     - X_train, y_train: Features and target variables for the training set.
#     - X_test, y_test: Features and target variables for the testing set.
#     """

#     # Split the data into feature data and target data
#     # Ensure proper date formatting (this is optional based on your use case)
#     # If you still need date filtering, you can uncomment and modify below as necessary
#     # if type(feature_start) != datetime.date:
#     #     feature_start = datetime.strptime(feature_start, '%Y-%m-%d').date()
#     # if type(feature_end) != datetime.date:
#     #     feature_end = datetime.strptime(feature_end, '%Y-%m-%d').date()
#     # if type(target_start) != datetime.date:
#     #     target_start = datetime.strptime(target_start, '%Y-%m-%d').date()
#     # if type(target_end) != datetime.date:
#     #     target_end = datetime.strptime(target_end, '%Y-%m-%d').date()

#     # Transactions data features
#     total_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('total_revenue'))
#     recency = data.groupBy('CustomerID').agg((F.max('date') - F.min('date')).alias('recency'))
#     recency = recency.withColumn('recency', recency['recency'].cast(IntegerType()))
#     frequency = data.groupBy('CustomerID').agg(F.count('InvoiceNo').alias('frequency'))
#     t = data.groupBy('CustomerID').agg((F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min('date'))).alias('t'))
#     time_between = t.join(frequency, 'CustomerID').withColumn('time_between', (t['t'] / frequency['frequency']))
#     avg_basket_value = total_rev.join(frequency, 'CustomerID').withColumn('avg_basket_value', total_rev['total_revenue'] / frequency['frequency'])
#     avg_basket_size = data.groupBy('CustomerID').agg((F.sum('Quantity') / F.count('InvoiceNo')).alias('avg_basket_size'))
#     returns = data.filter(data['Revenue'] < 0).groupBy('CustomerID').agg(F.count('InvoiceNo').alias('num_returns'))
#     hour = data.groupBy('CustomerID').agg(F.percentile_approx('hour', 0.5).alias('purchase_hour_med'))
#     dow = data.groupBy('CustomerID').agg(F.percentile_approx('dayofweek', 0.5).alias('purchase_dow_med'))
#     weekend = data.groupBy('CustomerID').agg(F.avg('weekend').alias('purchase_weekend_prop'))

#     # Combine all features into one DataFrame
#     feature_data = total_rev.join(recency, 'CustomerID', 'left') \
#                             .join(frequency, 'CustomerID', 'left') \
#                             .join(t, 'CustomerID', 'left') \
#                             .join(time_between, 'CustomerID', 'left') \
#                             .join(avg_basket_value, 'CustomerID', 'left') \
#                             .join(avg_basket_size, 'CustomerID', 'left') \
#                             .join(returns, 'CustomerID', 'left') \
#                             .join(hour, 'CustomerID', 'left') \
#                             .join(dow, 'CustomerID', 'left') \
#                             .join(weekend, 'CustomerID', 'left')

#     # Handle missing values by filling with 0
#     feature_data = feature_data.na.fill(0)

#     # Target data (revenue for the target period)
#     target_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('target_rev'))

#     # Join the target to the feature data
#     final_data = feature_data.join(target_rev, 'CustomerID', 'left').na.fill(0)

#     # Split data into training and testing datasets based on percentage
#     train_data, test_data = final_data.randomSplit([feature_percentage, 1 - feature_percentage], seed=42)

#     # Convert Spark DataFrame to pandas DataFrame for training and testing sets
#     train_data_pandas = train_data.toPandas()
#     test_data_pandas = test_data.toPandas()

#     # Split features (X) and target (y) for both training and testing sets
#     X_train = train_data_pandas.drop('target_rev', axis=1)  # Features for training
#     y_train = train_data_pandas['target_rev']  # Target for training
#     X_test = test_data_pandas.drop('target_rev', axis=1)  # Features for testing
#     y_test = test_data_pandas['target_rev']  # Target for testing

#     # Return the training and testing datasets
#     return X_train, y_train, X_test, y_test

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FeatureExtraction").getOrCreate()

# def get_features_spark_to_pandas(data, feature_percentage=0.8):
#     """
#     Extracts features for a predictive model and splits the data into 
#     training and testing datasets based on a given percentage.

#     Parameters:
#     - data: Spark DataFrame containing the raw dataset.
#     - feature_percentage: Percentage of the data to be used for training (default 80%).

#     Returns:
#     - X_train, y_train: Features and target variables for the training set.
#     - X_test, y_test: Features and target variables for the testing set.
#     """
    
#     data = data.withColumn('InvoiceDate', F.to_date('InvoiceDate', 'yyyy-MM-dd'))
#     # Transactions data features
#     total_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('total_revenue'))

#     # Replace 'date' with 'InvoiceDate' in the following calculations
#     recency = data.groupBy('CustomerID').agg((F.max('InvoiceDate') - F.min('InvoiceDate')).alias('recency'))
#     recency = recency.withColumn('recency', recency['recency'].cast(IntegerType()))
    
#     frequency = data.groupBy('CustomerID').agg(F.count('InvoiceNo').alias('frequency'))
    
#     t = data.groupBy('CustomerID').agg((F.datediff(F.lit(datetime(2011, 6, 11).date()), F.min('InvoiceDate'))).alias('t'))
    
#     time_between = t.join(frequency, 'CustomerID').withColumn('time_between', (t['t'] / frequency['frequency']))
    
#     avg_basket_value = total_rev.join(frequency, 'CustomerID').withColumn('avg_basket_value', total_rev['total_revenue'] / frequency['frequency'])
    
#     avg_basket_size = data.groupBy('CustomerID').agg((F.sum('Quantity') / F.count('InvoiceNo')).alias('avg_basket_size'))
    
#     returns = data.filter(data['Revenue'] < 0).groupBy('CustomerID').agg(F.count('InvoiceNo').alias('num_returns'))
    
#     hour = data.groupBy('CustomerID').agg(F.percentile_approx('hour', 0.5).alias('purchase_hour_med'))
    
#     dow = data.groupBy('CustomerID').agg(F.percentile_approx('dayofweek', 0.5).alias('purchase_dow_med'))
    
#     weekend = data.groupBy('CustomerID').agg(F.avg('weekend').alias('purchase_weekend_prop'))

#     # Combine all features into one DataFrame
#     feature_data = total_rev.join(recency, 'CustomerID', 'left') \
#                             .join(frequency, 'CustomerID', 'left') \
#                             .join(t, 'CustomerID', 'left') \
#                             .join(time_between, 'CustomerID', 'left') \
#                             .join(avg_basket_value, 'CustomerID', 'left') \
#                             .join(avg_basket_size, 'CustomerID', 'left') \
#                             .join(returns, 'CustomerID', 'left') \
#                             .join(hour, 'CustomerID', 'left') \
#                             .join(dow, 'CustomerID', 'left') \
#                             .join(weekend, 'CustomerID', 'left')

#     # Handle missing values by filling with 0
#     feature_data = feature_data.na.fill(0)

#     # Target data (revenue for the target period)
#     target_rev = data.groupBy('CustomerID').agg(F.sum('Revenue').alias('target_rev'))

#     # Join the target to the feature data
#     final_data = feature_data.join(target_rev, 'CustomerID', 'left').na.fill(0)

#     # Split data into training and testing datasets based on percentage
#     train_data, test_data = final_data.randomSplit([feature_percentage, 1 - feature_percentage], seed=42)

#     # Convert Spark DataFrame to pandas DataFrame for training and testing sets
#     train_data_pandas = train_data.toPandas()
#     test_data_pandas = test_data.toPandas()

#     # Split features (X) and target (y) for both training and testing sets
#     X_train = train_data_pandas.drop('target_rev', axis=1)  # Features for training
#     y_train = train_data_pandas['target_rev']  # Target for training
#     X_test = test_data_pandas.drop('target_rev', axis=1)  # Features for testing
#     y_test = test_data_pandas['target_rev']  # Target for testing

#     # Return the training and testing datasets
#     return X_train, y_train, X_test, y_test

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime

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


def spark_processing():
    """
    Main program for data processing.
    """
    # Create SparkSession
    spark = create_spark_session()

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
    # X_train = processed_df.drop("Revenue").toPandas()  # Example transformation for model input
    # y_train = processed_df.select("Revenue").toPandas()  # Example target column

    # Call the function to load and fine-tune the model
    path = '/home/nhtrung/CLV-Big-Data-Project/stream_process/model/CLV_V3.keras'
    load_and_finetune_model(path, X_train, y_train)

    print("Processing complete!")


if __name__ == "__main__":
    spark_processing()
