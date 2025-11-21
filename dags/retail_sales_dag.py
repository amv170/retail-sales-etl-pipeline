""" Retail Sales ETL DAG
-------------------------
This Airflow DAG orchestrates the ETL pipeline for the retail sales dataset. 
For now, it only runs the "extract" phase.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os

# Add scripts folder to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), "../scripts"))

from extract import extract_data
from transform import transform_data
from load import load_data


# Default settings for all tasks
default_args = {
    "owner": "Alvin",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 13),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define on the DAG
with DAG(
    dag_id="retail_sales_pipeline",
    default_args=default_args,
    description = "ETL pipeline for retail sales data",
    schedule_interval = "@daily",
    catchup = False,
) as dag:
    
    # Task 1: Extract raw data
    extract_task = PythonOperator(
        task_id = "extract_data",
        python_callable = extract_data,
    )
    
    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id = "transform_data",
        python_callable = transform_data,
    )
    
    # Task 3: Load data
    load_task = PythonOperator(
        task_id = "load_data",
        python_callable = load_data,
    )
    
    extract_task >> transform_task >> load_task