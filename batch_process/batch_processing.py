# batch_layer.py

from batch_process.spark.spark_scripts.spark_processing import clean_and_process_data
from batch_process.postgres.save_preprocessed_data import save_data

def batch_layer():
    try:
        print("Starting Spark processing...")
        data = clean_and_process_data()  # Fetch data using Spark
        print("Spark processing completed.")

        print("Saving data to PostgreSQL...")
        save_data(data)  # Save the processed data to PostgreSQL
        print("Data saved successfully.")
    except Exception as e:
        print(f"Error in batch_layer: {str(e)}")
