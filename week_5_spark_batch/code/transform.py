import pandas as pd
import pyarrow.parquet as pq
import argparse

def main(params):
    print(f'Initialize transformation {params.parquet}')
    parq = pq.read_table(params.parquet)
    df = parq.to_pandas()
    df.to_csv(params.csv, index=False)

    print(f'Finish transformed to {params.csv}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Transform parquet to csv')

    parser.add_argument('--parquet', help='parquet directory')
    parser.add_argument('--csv', help='csv directory')

    args = parser.parse_args()
    # print(args.parquet, args.csv)
    main(args)