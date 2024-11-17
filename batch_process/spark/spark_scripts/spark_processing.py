import pandas as pd
from hdfs import InsecureClient
from io import BytesIO

# Step 1: Connect to HDFS and read the data
def read_data_from_hdfs():
    hdfs_host = 'localhost'
    hdfs_port = 50070
    hdfs_user = 'nhtrung'
    file_path = '/batch-layer/raw_data.csv'

    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}', user=hdfs_user)
    
    try:
        # Read data from HDFS into a DataFrame
        with client.read(file_path) as reader:
            df = pd.read_csv(reader)
        print("Data loaded from HDFS successfully.")
        return df
    except Exception as e:
        print(f"Error reading data from HDFS: {e}")
        return pd.DataFrame()

# Step 2: Data Cleaning and Preprocessing
def clean_and_process_data():

    df = read_data_from_hdfs()

    if df.empty:
        print("No data to process.")
        return df

    # Convert 'InvoiceDate' to datetime format
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')

    # Drop rows where CustomerID is null
    df_cleaned = df.dropna(subset=['CustomerID'])
    df_cleaned['CustomerID'] = df_cleaned['CustomerID'].astype(str)

    # Remove rows with non-positive quantities and prices
    df_cleaned = df_cleaned[(df_cleaned['Quantity'] > 0) & (df_cleaned['UnitPrice'] > 0)]

    # Display the cleaned data summary
    print(f"Remaining rows after cleaning: {df_cleaned.shape[0]}")
    print("Summary Statistics:")
    print(df_cleaned.describe())

    return df_cleaned


# Main function
def main():
    print("Starting data processing...")
    cleaned_data = clean_and_process_data()
    
    if not cleaned_data.empty:
        print("Data processing completed successfully.")
    else:
        print("Data processing failed or no data to process.")

# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()