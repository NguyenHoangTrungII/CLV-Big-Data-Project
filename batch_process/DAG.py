# batch_layer

import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from batch_process.batch_processing import batch_layer  # Import the function

with DAG(
    dag_id="daily_data_sync",
    start_date=datetime.datetime(2024, 3, 29),
    schedule="*/1 * * * *",  # Run every 1 minute (using the `schedule` parameter)
) as dag:
    batch_layer_task = PythonOperator(
        task_id="batchlayer",
        python_callable=batch_layer  # Pass the function reference (without parentheses)
    )

batch_layer_task
