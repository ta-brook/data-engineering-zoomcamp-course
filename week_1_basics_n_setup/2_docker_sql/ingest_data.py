#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parq_name = 'output.parquet'
    csv_name = 'output.csv'

    # download the csv
    os.system(f"wget {url} -O {parq_name}")

    # transform parquet to csv
    parq = pd.read_parquet(parq_name)
    parq.to_csv(csv_name, index=False)
    print(f'transformed data to csv... name:{csv_name}')

    # db connection
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name,
                        con=engine, if_exists='replace')
    print(f'Create columns...')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    print(f'Initialize pandas iteration data ingestion...')
    while True:
        s_time = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        e_time = time()

        print(f'inserted another chunks..., took {e_time-s_time:.3f} second')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='user password for postgres')
    parser.add_argument('--host', help='user host for postgres')
    parser.add_argument('--port', help='user port for postgres')
    parser.add_argument('--db', help='user db for postgres')
    parser.add_argument('--table_name', help='use table_name for postgres')
    parser.add_argument('--url', help='use url for csv')

    args = parser.parse_args()

    main(args)
