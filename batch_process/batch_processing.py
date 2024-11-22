# batch_layer.py

from batch_process.spark.spark_scripts.spark_processing import spark_processing


def batch_layer():
    try:
        print("Starting Spark processing...")
        spark_processing()  # Fetch data using Spark
        print("Spark processing completed.")

        # print("Saving data to PostgreSQL...")
        # save_data(data)  # Save the processed data to PostgreSQL
        # print("Data saved successfully.")
    except Exception as e:
        print(f"Error in batch_layer: {str(e)}")
