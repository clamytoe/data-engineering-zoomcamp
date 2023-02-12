from pathlib import Path
from typing import Iterable

from download_datasets import LOCAL_DIR, dataset_generator, fetch
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_path = Path("data", "fhv")
    gcs_file_path = gcs_path / path.name
    gcs_block = GcsBucket.load("dtc-de-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=gcs_file_path)  # type: ignore


@flow()
def etl_web_to_gcs(years: list[int], months: Iterable[int]) -> None:
    """The main ETL function"""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    files = [fetch(file, True) for file in dataset_generator(years, months)]
    for file in files:
        write_gcs(file)


if __name__ == "__main__":
    years = [2019]
    months = range(1, 13)
    etl_web_to_gcs(years, months)
