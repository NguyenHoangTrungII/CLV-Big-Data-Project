import sys
import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append('/usr/local/airflow')

from batch_process.batch_processing import batch_layer  # Import the function
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
current_date = datetime.datetime.now(local_tz)

with DAG(
    dag_id="batch_sync",
    start_date=current_date,
    schedule="*/5 * * * *",  # Run every 1 minute (using the `schedule` parameter)
) as dag:
    batch_layer_task = PythonOperator(
        task_id="batchlayer",
        python_callable=batch_layer  # Pass the function reference (without parentheses)
    )

batch_layer_task
