from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import requests
import os
import io
import zipfile
from airflow.operators.python import PythonOperator

YANDEX_PUBLIC_URL  = "https://disk.yandex.ru/d/bhf2M8C557AFVw"
LOCAL_FILE_PATH = "/opt/jobs"
TARGET_FILE_NAME = "russian_houses.csv"

def find_nested_zip(root_dir, exclude_path):
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith(".zip"):
                candidate = os.path.join(dirpath, filename)
                if candidate != exclude_path:
                    return candidate
    return None

def download_yandex_csv(**context):
    file_path = os.path.join(LOCAL_FILE_PATH, TARGET_FILE_NAME)

    if os.path.exists(file_path):
        print(f"file {TARGET_FILE_NAME} exists, miss downloading")
        return

    api_url = "https://cloud-api.yandex.net/v1/disk/public/resources/download"
    params = {"public_key": YANDEX_PUBLIC_URL }

    response = requests.get(api_url, params=params)
    response.raise_for_status()

    download_url = response.json().get("href")
    archive_path = os.path.join(LOCAL_FILE_PATH, "temp_archive.zip" )

    archive_response = requests.get(download_url, stream=True)
    archive_response.raise_for_status()

    nested_zip = find_nested_zip(LOCAL_FILE_PATH, archive_path)
    if nested_zip:
        with zipfile.ZipFile(nested_zip, 'r') as z:
            z.extractall(LOCAL_FILE_PATH)
        os.remove(nested_zip)


    with open(archive_path, "wb") as f:
        for chunk in archive_response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    with zipfile.ZipFile(archive_path, 'r') as z:
        z.extractall(LOCAL_FILE_PATH)

    nested_zip = find_nested_zip(LOCAL_FILE_PATH, archive_path)
    if nested_zip:
        with zipfile.ZipFile(nested_zip, 'r') as z:
            z.extractall(LOCAL_FILE_PATH)
        os.remove(nested_zip)
        
    if os.path.exists(archive_path):
        os.remove(archive_path)
with DAG(
    dag_id="russian_houses_dag",
    tags=['hose_data'],
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id = "download_yandex_csv",
        python_callable=download_yandex_csv
    )
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

    download_task >> submit_spark_job