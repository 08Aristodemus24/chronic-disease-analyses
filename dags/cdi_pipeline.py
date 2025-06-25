import sys
import datetime as dt
import os
import shutil
import time
import boto3
import duckdb

from pathlib import Path

from airflow import DAG, settings
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection 
from airflow.configuration import conf

# go to this site if you want to use cron instead of datetime to set schedule_interval
# https://crontab.guru/#00_12_*_*_Sun




# get airflow folder
AIRFLOW_HOME = conf.get('core', 'dags_folder')

# base dir would be /opt/***/ or /opt/airflow
BASE_DIR = Path(AIRFLOW_HOME).resolve().parent

default_args = {
    'owner': 'mikhail',
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=2)
}

with DAG(
    dag_id="cdi_pipeline",
    default_args=default_args,
    description="extract transfrom load chronic disease indicator data and population data from source to duckdb",
    start_date=dt.datetime(2024, 1, 1, 12),

    # runs every sunday at 12:00 
    schedule="00 12 * * Sun",
    catchup=False
) as dag:
    
    extract_cdi = BashOperator(
        task_id="extract_cdi",
        bash_command=f"python {AIRFLOW_HOME}/operators/extract_cdi.py -L https://www.kaggle.com/api/v1/datasets/download/payamamanat/us-chronic-disease-indicators-cdi-2023"
    )

    extract_populations = BashOperator(
        task_id="extract_populations",
        bash_command=f"python {AIRFLOW_HOME}/operators/extract_us_population_per_state_by_sex_age_race_ho.py"
    )

    # [extract_cdi, extract_populations]
    extract_populations