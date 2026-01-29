import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # DummyOperator is deprecated, use EmptyOperator instead

import sys
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent  # airflow/dags
PROJECT_ROOT = BASE_DIR.parent.parent       
sys.path.append(str(PROJECT_ROOT))

from scripts.extract_load import extract_load
from scripts.raw_to_processed import main as transform_data
from scripts.train import main
from scripts.predict import main

# Default arguments for the DAG
default_args = {
    "owner": "vnp_kdl",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

###############################################
# Parameters & Arguments
###############################################
from dotenv import load_dotenv
load_dotenv(".env")

MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
###############################################


with DAG("elt_train_predict_pipeline", start_date=datetime(2025, 9, 1), schedule=None, default_args=default_args) as dag:  # schedule="@daily"/"@monthly" if needed

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=extract_load,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    train_model = PythonOperator(
        task_id="train_model",
        python_callable=main
    )

    predict = PythonOperator(
        task_id="predict",
        python_callable=main
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline"
    )

start_pipeline >> extract_load >> transform_data >> train_model >> predict >> end_pipeline
