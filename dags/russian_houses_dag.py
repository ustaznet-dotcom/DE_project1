from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="russian_houses_dag",
    tags=['hose_data'],
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        conn_id='spark_default',  # Connection ID configured in Airflow UI
        application='/opt/jobs/main.py',  # Path to your Spark application
        name='airflow_spark_job',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='1600m',
        driver_memory='1g',
        verbose=True
    )