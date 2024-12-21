# import sys
# import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from batch_process.batch_processing import batch_layer  # Import the function

# with DAG(
#     dag_id="batch_sync",
#     start_date=datetime.datetime(2024, 3, 29),
#     schedule="*/5 * * * *",  # Run every 1 minute (using the `schedule` parameter)
# ) as dag:
#     batch_layer_task = PythonOperator(
#         task_id="batchlayer",
#         python_callable=batch_layer  # Pass the function reference (without parentheses)
#     )

# batch_layer_task


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('example_dag', default_args=default_args, schedule_interval='@daily') as dag:
    start_task = DummyOperator(task_id='start_task')
