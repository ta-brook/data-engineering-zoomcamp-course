import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

'''
January     February    March
.......... | ......... | ......... |
wait for month over then execute
'''

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    dag_id="LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
)

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}' 


with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sS {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ls_task = BashOperator(
        task_id='ls',
        bash_command=f'ls {AIRFLOW_HOME}'
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE.replace('parquet', 'csv'),
            parq_file=OUTPUT_FILE_TEMPLATE
        )
    )

    wget_task >>  ingest_task >> ls_task
