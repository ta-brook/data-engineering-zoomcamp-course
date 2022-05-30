import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from dags_script import format_to_parquet
from dags_script import upload_to_gcs
from dags_script import csv_to_parquet

def download_parquetize_upload_dag(
    dag,
    url_template,
    local_csv_path_template,
    local_parquet_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id='curl_task',
            bash_command=f'curl -sSLf {url_template} > {local_parquet_template}'
        )

        parquetize_data = PythonOperator(
            task_id='parquetize_task',
            python_callable=format_to_parquet,
            op_kwargs=dict(
                src_file=local_parquet_template
            )
        )

        update_data_lake = PythonOperator(
            task_id='ingest_data_lake_task',
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": f"{local_parquet_template}",
            },
        )

        rm_task = BashOperator(
            task_id='rm_task',
            # bash_command=f'rm {AIRFLOW_HOME}/{CSV_FILENAME_TEMPLATE} {AIRFLOW_HOME}/{PARQUET_FILENAME_TEMPLATE}'
            bash_command=f'rm {local_parquet_template}'
        )

        download_dataset_task >> parquetize_data >> update_data_lake >> rm_task


AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

YELLOW_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_OUTPUT_FILE_TEMPLATE_CSV = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

PARQUET_FILENAME_TEMPLATE = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
CSV_FILENAME_TEMPLATE = PARQUET_FILENAME_TEMPLATE.replace('parquet', 'csv')
# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}' 

YELLOW_TAXI_GCS_PATH_TEMPLATE = 'raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

yellow_taxi_data = DAG(
    dag_id="data_ingestion_yellow_taxi_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021,1,1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag = yellow_taxi_data,
    url_template = YELLOW_TAXI_URL_TEMPLATE,
    local_csv_path_template = YELLOW_TAXI_OUTPUT_FILE_TEMPLATE_CSV,
    local_parquet_template = YELLOW_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET,
    gcs_path_template = YELLOW_TAXI_GCS_PATH_TEMPLATE
)



# https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2019-01.parquet

GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_OUTPUT_FILE_TEMPLATE_CSV = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_TAXI_GCS_PATH_TEMPLATE = 'raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

green_taxi_data = DAG(
    dag_id="data_ingestion_green_taxi_dag",
    schedule_interval="0 7 2 * *",
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,1,1),
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag = green_taxi_data,
    url_template = GREEN_TAXI_URL_TEMPLATE,
    local_csv_path_template = GREEN_TAXI_OUTPUT_FILE_TEMPLATE_CSV,
    local_parquet_template = GREEN_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET,
    gcs_path_template = GREEN_TAXI_GCS_PATH_TEMPLATE
)

# https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-01.parquet

FHV_TAXI_URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_OUTPUT_FILE_TEMPLATE_CSV = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_TAXI_GCS_PATH_TEMPLATE = 'raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

fhv_data = DAG(
    dag_id="data_ingestion_fhv_dag_v02",
    schedule_interval="0 8 2 * *",
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,1,1),
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
)

download_parquetize_upload_dag(
    dag = fhv_data,
    url_template = FHV_TAXI_URL_TEMPLATE,
    local_csv_path_template = FHV_TAXI_OUTPUT_FILE_TEMPLATE_CSV,
    local_parquet_template = FHV_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET,
    gcs_path_template = FHV_TAXI_GCS_PATH_TEMPLATE
)




# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

ZONES_TAXI_URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
ZONES_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET = AIRFLOW_HOME + '/taxi+_zone_lookup.parquet'
ZONES_TAXI_OUTPUT_FILE_TEMPLATE_CSV = AIRFLOW_HOME + '/taxi+_zone_lookup.csv'
ZONES_TAXI_GCS_PATH_TEMPLATE = 'raw/zones/taxi+_zone_lookup.parquet'

zones_data = DAG(
    dag_id="data_ingestion_zones_dag",
    schedule_interval='@once',
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
)

with zones_data :
    download_dataset_task = BashOperator(
        task_id='curl_task',
        bash_command=f'curl -sSLf {ZONES_TAXI_URL_TEMPLATE} > {ZONES_TAXI_OUTPUT_FILE_TEMPLATE_CSV}'
    )

    parquetize_data = PythonOperator(
        task_id='parquetize_task',
        python_callable=csv_to_parquet,
        op_kwargs=dict(
            src_file=ZONES_TAXI_OUTPUT_FILE_TEMPLATE_CSV,
            output_file=ZONES_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET
        )
    )

    update_data_lake = PythonOperator(
        task_id='ingest_data_lake_task',
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": ZONES_TAXI_GCS_PATH_TEMPLATE,
            "local_file": f"{ZONES_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET }",
        },
    )

    rm_task = BashOperator(
        task_id='rm_task',
        bash_command=f'rm {ZONES_TAXI_OUTPUT_FILE_TEMPLATE_CSV} {ZONES_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET }'
        # bash_command=f'rm {AIRFLOW_HOME}/{ZONES_TAXI_OUTPUT_FILE_TEMPLATE_PARQUET }'
    )

    download_dataset_task >> parquetize_data >> update_data_lake >> rm_task