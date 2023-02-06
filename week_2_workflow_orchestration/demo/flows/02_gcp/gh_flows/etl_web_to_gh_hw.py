from pathlib import Path

import pandas as pd
from prefect import Flow, flow, task
from prefect.filesystems import GitHub
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    local_dir = Path("data", color)
    local_dir.mkdir(parents=True, exist_ok=True)
    path = local_dir / f"{dataset_file}.parquet"
    df.to_parquet(path, compression="gzip")  # type: ignore
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-de-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)  # type: ignore
    return


with Flow("clamytoe-dez") as etl_web_to_gcs:  # type: ignore
    """The main ETL function"""
    color = "green"
    year = 2020
    month = 11
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


flow.storage = GitHub(
    repository="https://github.com/clamytoe/data-engineering-zoomcamp",
    path="week_2_workflow_orchestration/demo/flows/02_gcp/gh_flow",  # type: ignore
)
