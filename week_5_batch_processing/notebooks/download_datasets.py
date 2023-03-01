from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, Iterable

import pandas as pd
import wget


@dataclass
class Taxi:
    """Taxi parameter class

    This parameter class object is used to generate the file urls needed to
    download its complete dataset.
    """

    taxi_type: str
    years: list[int] = field(default_factory=list)
    months: Iterable = field(default_factory=list)
    max_year: int = 2021
    max_month: int = 7
    base_url: str = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    file_name: str = "{taxi_type}_tripdata_{year}-{month}.csv.gz"
    local_dir: Path = Path("data", "raw")

    def __post_init__(self):
        self.dataset = [file for file in self.dataset_generator()]

    def convert_to_parquet(self, url: str, local_file: Path) -> Path:
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

    def dataset_generator(self) -> Generator:
        """Generates a generator of dataset file names

        Args:
            params (Taxi): Taxi object

        Yields:
            Generator: The file name in the format of fhv_tripdata_YEAR-MONTH.csv.gz
        """
        for year in self.years:
            for month in self.months:
                if year == self.max_year and month > self.max_month:
                    continue
                else:
                    month = str(month).rjust(2, "0")
                    yield self.file_name.format(
                        year=year,
                        month=month,
                        taxi_type=self.taxi_type,
                    ), year, month

    def download(self, to_parquet: bool = False) -> list[Path]:
        """Downloads the files

        Args:
            to_parquet (bool, optional): Flag to download in the parquet format.
            Defaults to False.

        Returns:
            list[Path]: A list of the files that were downloaded.
        """
        retrieved = [
            self.fetch(file, year, month, to_parquet)
            for file, year, month in self.dataset
        ]
        return retrieved

    def fetch(self, file: str, year: int, month: str, to_parquet: bool = False) -> Path:
        """Download datasets from GitHub

        Args:
            file (str): The file to download
            taxi_type (str): The type of taxi data, i.e. yellow, green, or fhv
            to_parquet (bool): Whether or not to save the file to parquet format

        Returns:
            A path object for the file that was downloaded
        """
        local_dir = self.local_dir / f"{self.taxi_type}/{year}/{month}"
        local_dir.mkdir(parents=True, exist_ok=True)
        local_file = local_dir / file

        url = f"{self.base_url}/{self.taxi_type}/{file}"
        if not to_parquet:
            if not local_file.exists():
                print(f"Downloading: {local_file}")
                wget.download(url, out=str(local_file))
                print()
            else:
                print(f"{file} already downloaded...")
        else:
            local_file = self.convert_to_parquet(url, local_file)

        return local_file


if __name__ == "__main__":
    # all years and months
    all_years = [2019, 2020, 2021]
    all_months = range(1, 13)

    # instantiate the taxi types
    yellow = Taxi("yellow", all_years, all_months)
    green = Taxi("green", all_years, all_months)
    fhv = Taxi("fhv", all_years, all_months)
    # fhvhv = Taxi("fhvhv", [2021], [1, 6])

    # combine them to download at once
    taxi_types = (yellow, green, fhv)

    # dowload the files
    files = []
    for taxi in taxi_types:
        files += taxi.download()

    # print out a result
    print("Downloaded the following files:")
    for file in files:
        print(f"  {file}")
