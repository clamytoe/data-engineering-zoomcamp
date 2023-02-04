from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    local_dir = Path("..", "data", color)
    local_dir.mkdir(parents=True, exist_ok=True)
    gcs_filename = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = Path("data", color, gcs_filename)
    gcs_block = GcsBucket.load("dtc-de-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=local_dir)  # type: ignore
    return Path(gcs_path)


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("dtc-de-gcp-creds")

    df.to_gbq(
        destination_table="ny_taxi.rides",
        project_id="dtc-de-course-374214",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),  # type: ignore
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
