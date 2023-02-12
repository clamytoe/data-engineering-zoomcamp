from pathlib import Path
from typing import Generator, Iterable

import pandas as pd
import wget

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"
FILE_NAME = "fhv_tripdata_{year}-{month}.csv.gz"
LOCAL_DIR = Path("data")
MAX_YEAR = 2021
MAX_MONTH = 7


def convert_to_parquet(url: str, local_file: Path) -> Path:
    """Converts CSV files to parquet format

    Args:
        url (str): The url for the file to download
        local_file (Path): The local file path to save the file to

    Returns:
        Path: The path to the saved file
    """
    p = Path(str(local_file).removesuffix("".join(local_file.suffixes)))
    p_file = p.with_suffix(".parquet.gz")
    print(f"Converting {local_file} to {p_file}...")
    if not p_file.exists():
        df = pd.read_csv(url)
        df.to_parquet(p_file, engine="pyarrow", compression="gzip")

    return p_file


def dataset_generator(years: list[int], months: Iterable[int]) -> Generator:
    """Generates a generator of dataset file names

    Args:
        years (list[int]): The years to generate names for
        months (Iterable[int]): The months to generate names for

    Yields:
        Generator: The file name in the format of fhv_tripdata_YEAR-MONTH.csv.gz
    """
    for year in years:
        for month in months:
            if year == MAX_YEAR and month > MAX_MONTH:
                continue
            else:
                yield FILE_NAME.format(year=year, month=str(month).rjust(2, "0"))


def fetch(file: str, to_parquet: bool = False) -> Path:
    """Download datasets from GitHub

    Args:
        file (str): The file to download
        to_parquet (bool): Whether or not to save the file to parquet format

    Returns:
        A path object for the file that was downloaded
    """
    local_file = LOCAL_DIR / file
    url = BASE_URL + file
    if not to_parquet:
        if not local_file.exists():
            print(f"Doanloading: {local_file}")
            wget.download(url, out=str(local_file))
            print()
        else:
            print(f"{file} already downloaded...")
    else:
        local_file = convert_to_parquet(url, local_file)

    return local_file


def main():
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    years = [2019]
    months = range(1, 13)
    for file in dataset_generator(years, months):
        fetch(file, True)


if __name__ == "__main__":
    main()
