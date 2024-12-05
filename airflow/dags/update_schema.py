from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
from airflow.decorators import task

SCHEMA_VERSION = "1.0"
SCHEMA_VERSION_KEY = "floodnet_schema_version"

with DAG(
    'floodnet_schema_management',
    description='Manage FloodNet database schema',
    schedule_interval=None,  # Manual triggers only
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    setup_schema = PostgresOperator(
        task_id='setup_schema',
        postgres_conn_id='postgres_results',
        sql=Path("/opt/airflow/src/sql/schema.sql").read_text(),
    )

    @task
    def update_schema_version():
        Variable.set(SCHEMA_VERSION_KEY, SCHEMA_VERSION)
        return "Schema version updated"

    setup_schema >> update_schema_version()
