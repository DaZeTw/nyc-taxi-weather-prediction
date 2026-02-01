import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

from scripts.extract_load import extract_load
from scripts.transform_data import transform_data
from scripts.validate_data import validate_data  
from scripts.merge_weather_data import merge_weather_data
from scripts.convert_to_delta import delta_convert

# Default arguments for the DAG
default_args = {
    "owner": "dazetw",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "dazetw@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

###############################################
# Parameters & Arguments
###############################################
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']

# Get processing configuration from Airflow Variables
PROCESSING_YEARS = json.loads(Variable.get("processing_years", default_var="[2024]"))
###############################################


with DAG(
    "nyc_taxi_weather_elt_pipeline", 
    start_date=datetime(2024, 1, 1), 
    schedule=None, 
    default_args=default_args,
    description="NYC Taxi + Weather ELT Pipeline: Extract → Transform → Merge Weather → Validate → Delta Lake",
    catchup=False,
    tags=['nyc-taxi', 'weather', 'elt', 'delta-lake']
) as dag:

    start_pipeline = DummyOperator(
        task_id="start_pipeline"
    )

    # ==========================================
    # Step 1: Extract & Load raw data to bucket_1 (landing)
    # ==========================================
    extract_load_task = PythonOperator(
        task_id="extract_load",
        python_callable=extract_load,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY,
            'years': PROCESSING_YEARS
        }
    )

    # ==========================================
    # Step 2: Transform data → bucket_2/batch (processed, no weather)
    # ==========================================
    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY,
            'years': PROCESSING_YEARS
        }
    )

    # ==========================================
    # Step 3: Merge weather data → bucket_2/batch_with_weather (silver layer)
    # ==========================================
    merge_weather_task = PythonOperator(
        task_id="merge_weather_data",
        python_callable=merge_weather_data,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY,
            'years': PROCESSING_YEARS
        }
    )

    # ==========================================
    # Step 4: Validate data quality → bucket_2/batch_validated
    # ==========================================
    validate_data_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY,
            'years': PROCESSING_YEARS
        }
    )

    # ==========================================
    # Step 5: Convert to Delta Lake → bucket_3/gold (gold layer with features)
    # ==========================================
    delta_convert_task = PythonOperator(
        task_id="convert_to_delta",
        python_callable=delta_convert,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY,
            'years': PROCESSING_YEARS
        }
    )

    end_pipeline = DummyOperator(
        task_id="end_pipeline"
    )

    # ==========================================
    # Pipeline Flow (Complete ELT Process)
    # ==========================================
    start_pipeline >> extract_load_task >> transform_data_task >> merge_weather_task >> validate_data_task >> delta_convert_task >> end_pipeline