import os 
import logging

from time import time
from sqlalchemy import create_engine
from google.cloud import storage
import pyarrow.csv as pv
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import csv

def format_to_parquet(src_file):
    if not src_file.endswith('.parquet'):
        logging.error("Can only accept source files in parquet format, for the moment")
        return
    table = pq.read_table(src_file)

    print(f'save data to both csv and parquet format...')

    pq.write_table(table, src_file)
    # csv.write_csv(table, f"{src_file.replace('parquet', 'csv')}")

    print(f'successfully save file...')

def csv_to_parquet(src_file, output_file):
    """
    Convert the provided csv file to parquet using PyArrow
    """
    if not src_file.endswith('.csv'):
        raise ValueError('The passed file is not in csv format.')
    table = pv.read_csv(src_file)
    pq.write_table(table, output_file)

# def upload_to_database(user, password, host, port, db, table_name, csv_file, parq_file):
#     print(table_name, parq_file, csv_file)

#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
#     engine.connect()
#     print(f'connection establish successfully, inserting data....')

#     df_iter = pd.read_csv(csv_file, iterator=True, chunksize=10000)


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)