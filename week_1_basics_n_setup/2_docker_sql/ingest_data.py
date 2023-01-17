#!/usr/bin/env python
# coding: utf-8

import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    if url.endswith(".csv.gz"):
        data_file_name = "output.csv.gz"
        read_data = read_csv
    elif url.endswith(".parquet"):
        data_file_name = "output.parquet"
        read_data = read_parquet
    else:
        data_file_name = "output.csv"
        read_data = read_csv

    os.system(f"wget -c {url} -O {data_file_name}")
    read_data(data_file_name, table_name, engine)


def read_csv(data_file_name, table_name, engine):
    df_iter = pd.read_csv(
        data_file_name,
        parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        iterator=True,
        chunksize=100_000,
        low_memory=False,
    )
    df = next(df_iter)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    print(f"Created table: {table_name}")

    t_start = time()
    df.to_sql(name=table_name, con=engine, if_exists="append")
    t_end = time()
    print(f"inserted first chunk, took {t_end - t_start:.3f} seconds")

    while True:
        t_start = time()

        try:
            df = next(df_iter)
            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print(f"inserted another chunk, took {t_end - t_start:.3f} second")
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


def read_parquet(data_file_name, table_name, engine):
    df = pd.read_parquet(data_file_name, engine="pyarrow")

    # create the table
    t_start = time()
    df.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")
    t_end = time()
    print(f"Created table: {table_name}")

    chunksize = 10_000
    max_size = df.shape[0]
    last_run = False
    start = 0
    current = chunksize

    print("Ingesting data...")
    t_start = time()
    # initialize progrogress bar
    with tqdm(total=max_size, unit="steps", unit_scale=True) as pbar:
        while not last_run:
            # insert chunks
            df.iloc[start:current].to_sql(
                name="yellow_taxi_data", con=engine, if_exists="append", method="multi"
            )

            start = current
            current += chunksize

            if current > max_size:
                current = max_size
                last_run = True
            pbar.update(chunksize)

    t_end = time()
    print(
        f"Finished ingesting data into the postgres database, {t_end - t_start:.3f} seconds"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file")

    args = parser.parse_args()

    main(args)
