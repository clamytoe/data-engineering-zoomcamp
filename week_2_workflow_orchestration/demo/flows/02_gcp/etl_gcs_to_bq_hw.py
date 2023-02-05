from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dtc-de-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="../data/")  # type: ignore
    return Path(gcs_path)


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("dtc-de-gcp-creds")

    df.to_gbq(
        destination_table="ny_taxi.rides",
        project_id="dtc-de-course-374214",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),  # type:ignore
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(months: list[int], year: int, color: str) -> None:
    """Main ETL flow to load data into Big Query"""
    total_rows = 0
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = pd.read_parquet(path)
        total_rows += df.shape[0]
        write_bq(df)
    print(f"Total number of rows processed: {total_rows}")


if __name__ == "__main__":
    months = [2, 3]
    color = "yellow"
    year = 2019
    etl_gcs_to_bq(months, year, color)
