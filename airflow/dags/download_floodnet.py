from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'floodnet_data_ingestion',
    default_args=default_args,
    description='Download FloodNet data',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    download_data = BashOperator(
        task_id='download_floodnet_data',
        bash_command="""
        LD_PRELOAD=/opt/conda/envs/flooding_data/lib/libstdc++.so.6 micromamba run -n flooding_data python /opt/airflow/src/scripts/download_floodnet_data.py
        """
    )

    download_data
