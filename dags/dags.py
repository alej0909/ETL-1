from dotenv import load_dotenv
import os
import sys

load_dotenv()
work_dir = os.getenv('WORK_DIR')
sys.path.append(work_dir)





from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from dags.etl import *



# Define default arguments for tasks
default_args = {
    'owner': 'Airflow_proyecto',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 6),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Define the DAG
with DAG(
    'Airflow_proyecto',
    default_args=default_args,
    description='workflow stadistics',
    schedule_interval='@daily',  
) as dag:
    
    extract_cardio = PythonOperator(
        task_id='extract_cardio',
        python_callable=extract_data_cardio,
    )

    validate_cardio_data  = PythonOperator(
        task_id='validate_cardio_data',
        python_callable=validate_cardio,
    )

    transform_cardio = PythonOperator(
        task_id='transform_cardio',
        python_callable=transform_cardio_data,
    )

    extract_deaths = PythonOperator(
        task_id='extract_deaths',
        python_callable=extract_data_deaths,
    )

    validate_deaths_data  = PythonOperator(
        task_id='validate_deaths_data',
        python_callable=validate_deaths,
    )

    extract_api = PythonOperator(
        task_id='extract_api',
        python_callable=extract_owid_data,
    )
    validate_api_data  = PythonOperator(
        task_id='validate_api_data',
        python_callable=validate_api,
    )

    transform_api = PythonOperator(
        task_id='transform_api',
        python_callable=transform_owid,
    )
    Merge = PythonOperator(
        task_id='Merge',
        python_callable=merge,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )

    producer = PythonOperator(
        task_id='producer',
        python_callable=producer_kafka,
    )

    

    extract_cardio >> validate_cardio_data >>transform_cardio >> load
    extract_deaths >> validate_deaths_data >> Merge >> load 
    extract_api >> validate_api_data >> transform_api >> Merge >> load >> producer
