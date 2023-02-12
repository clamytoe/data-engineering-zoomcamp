from pathlib import Path

from download_datasets import LOCAL_DIR, dataset_generator, fetch
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

# @task()
# def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
#     """Write DataFrame out locally as parquet file"""
#     local_dir = Path("data", color)
#     local_dir.mkdir(parents=True, exist_ok=True)
#     path = local_dir / f"{dataset_file}.parquet"
#     df.to_parquet(path, compression="gzip")
#     return path


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_path = Path("data", "fhv")
    gcs_file_path = gcs_path / path.name
    gcs_block = GcsBucket.load("dtc-de-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=gcs_file_path)  # type: ignore


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    years = [2019]
    months = range(1, 13)

    files = [fetch(file, True) for file in dataset_generator(years, months)]
    for file in files:
        write_gcs(file)


if __name__ == "__main__":
    etl_web_to_gcs()
