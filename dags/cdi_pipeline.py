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

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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

    extract_population = BashOperator(
        task_id="extract_population",
        bash_command=f"python {AIRFLOW_HOME}/operators/extract_us_population_per_state_by_sex_age_race_ho.py"
    )

    transform_cdi = SparkSubmitOperator(
        task_id="transform_cdi",
        conn_id="my_spark_conn",
        application="./dags/operators/transform_cdi.py",

        packages=",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.google.guava:guava:27.0-jre",
            "org.apache.httpcomponents:httpcore:4.4.16",

            # "com.amazonaws:aws-java-sdk-bundle:1.11.563",

            "com.amazonaws:aws-java-sdk-core:1.12.768",
            "com.amazonaws:aws-java-sdk-kms:1.12.768",
            "com.amazonaws:jmespath-java:1.12.768",
            "com.amazonaws:aws-java-sdk-s3:1.12.768"
        ]),
        verbose=True
    )

    # spark-submit \
    #     --master spark://spark-master:7077 \
    #     --name arrow-spark \
    #     --verbose ./dags/operators/transform_us_population_per_state_by_sex_age_race_ho.py \
    #     --year-range-list 2000-2009
    transform_population = SparkSubmitOperator(
        task_id="transform_population",
        conn_id="my_spark_conn",
        application="./dags/operators/transform_us_population_per_state_by_sex_age_race_ho.py",
        # conf={
        #     "spark.driver.userClassPathFirst": "true",
        #     "spark.executor.userClassPathFirst": "true"
        # },
        
        packages=",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.google.guava:guava:27.0-jre",
            "org.apache.httpcomponents:httpcore:4.4.16",

            # "com.amazonaws:aws-java-sdk-bundle:1.11.563",

            "com.amazonaws:aws-java-sdk-core:1.12.768",
            "com.amazonaws:aws-java-sdk-kms:1.12.768",
            "com.amazonaws:jmespath-java:1.12.768",
            "com.amazonaws:aws-java-sdk-s3:1.12.768"
        ]),

        # if we have explicitly downloaded the jar files in the jar
        # folder of where we installed spark in the container we can
        # specify the paths of those jar files
        # jars=",".join([
        #     "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar",
        #     "/opt/bitnami/spark/jars/guava-27.0-jre.jar",
        #     "/opt/bitnami/spark/jars/httpcore-4.4.16.jar",
        #     "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.563.jar",
        #     "/opt/bitnami/spark/jars/aws-java-sdk-core-1.11.563.jar",
        #     "/opt/bitnami/spark/jars/aws-java-sdk-kms-1.11.563.jar",
        #     "/opt/bitnami/spark/jars/jmespath-java-1.11.563.jar",
        #     "/opt/bitnami/spark/jars/aws-java-sdk-s3-1.11.563.jar"
        # ]),

        # pass argument vector to spark submit job operator since
        # it is a file that runs like a script
        application_args=["--year-range-list", "2000-2009", "2010-2019", "2020-2023"],
        verbose=True
    )

    load_primary_tables = BashOperator(
        task_id="load_primary_tables",
        bash_command=f"python {AIRFLOW_HOME}/operators/load_primary_tables.py"
    )

    update_tables = BashOperator(
        task_id="update_tables",
        bash_command=f"python {AIRFLOW_HOME}/operators/update_tables.py"
    )

    [extract_cdi, extract_population] #>> load_primary_tables
    
    extract_cdi >> transform_cdi
    extract_population >> transform_population

    [transform_cdi, transform_population] >> load_primary_tables

    load_primary_tables >> update_tables

