from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import sys
import os

# Add project root to path
sys.path.append("/opt/airflow")

from scripts.ingestion_pipeline import run_pipeline

# Default args
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id="football_etl_pipeline",
    default_args=default_args,
    description="End-to-End Football Pipeline",
    schedule_interval="0 3 * * 0",  # every sunday at 3 am
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # ingestion
    ingestion_task = BashOperator(
        task_id="ingestion_kaggle_to_s3",
        bash_command="python /opt/airflow/scripts/ingestion_pipeline.py",
        )

    # transformation (spark)
    transform_task = BashOperator(
        task_id="spark_transformation",
        bash_command="python /opt/airflow/scripts/production_pipeline.py",
    )

    # snowflake load
    snowflake_load = BashOperator(
        task_id="load_to_snowflake",
        bash_command="""
        snowsql -f /opt/airflow/sql/1_create_tables.sql &&
        snowsql -f /opt/airflow/sql/2_load_data.sql &&
        snowsql -f /opt/airflow/sql/3_gold_layer_tables.sql
        """,
    )

    # execution ordering
    ingestion_task >> transform_task >> snowflake_load