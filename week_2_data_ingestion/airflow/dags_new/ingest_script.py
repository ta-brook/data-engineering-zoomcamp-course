import os 

from time import time

import pandas as pd
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file, parq_file):
    print(table_name, parq_file, csv_file)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print(f'connection establish successfully, inserting data....')

    # transform parquet to csv
    parq = pd.read_parquet(parq_file)
    parq.to_csv(csv_file, index=False)
    print(f'transformed data to csv... name:{csv_file}')

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=10000)

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