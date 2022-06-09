import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

SHORT_DATASET = 'fhv'
DATASET = "fhv_tripdata"
COLOUR_RANGE = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_fhv",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    gcs_2_gcs = GCSToGCSOperator(
        task_id='gcs_2_gcs_task',
        source_bucket=BUCKET,
        source_object=f'{INPUT_PART}/{DATASET}/2019/{DATASET}*.{INPUT_FILETYPE }',
        destination_bucket=BUCKET,
        destination_object=f'{SHORT_DATASET}/{DATASET}',
        move_object=True
      )

    gcs_2_bq_ext = BigQueryCreateExternalTableOperator(
    task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"external_{DATASET}",
            },
            "externalDataConfiguration": {
                'autodetect':True,
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": [f"gs://{BUCKET}/{SHORT_DATASET}/{DATASET}*"],
            },
        }
    )

    # The partition table doesn't required for homework

    # CREATE_PART_TBL_QUERY = f'CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET} \
    # PARTITION BY \
    #   DATE(tpep_pickup_datetime) AS \
    # SELECT * FROM {BIGQUERY_DATASET}.external_yellow_tripdata;'

    # bq_ext_2_part = BigQueryInsertJobOperator(
    #     task_id="bq_ext_2_part_task",
    #     configuration={
    #         "query": {
    #             "query": CREATE_PART_TBL_QUERY,
    #             "useLegacySql": False,
    #         }
    #     },
    # )

    gcs_2_gcs >> gcs_2_bq_ext 