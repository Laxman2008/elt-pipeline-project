from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

ELT_SCHEDULE = os.environ.get("AIRFLOW_ELT_SCHEDULE", "@hourly")
default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="csv_to_clickhouse_elt",
    default_args=default_args,
    schedule_interval=ELT_SCHEDULE,
    start_date=datetime(2024,1,1),
    catchup=False,
    max_active_runs=1,
    tags=["elt","csv"]
) as dag:

    ingest = BashOperator(
        task_id="ingest_csv_to_postgres",
        bash_command="python /opt/etl/etl.py --mode ingest",
    )

    transform = BashOperator(
        task_id="transform_and_load",
        bash_command="python /opt/etl/etl.py --mode transform",
    )

    ingest >> transform
