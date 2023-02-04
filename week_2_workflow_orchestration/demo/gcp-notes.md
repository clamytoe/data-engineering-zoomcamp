# ETL with GCP & Prefect

## ETL from Web to Google Cloud Storage

### Start Orion server

Like we did in the previous lesson, we are going to start Orion.

```bash
prefect orion start

 ___ ___ ___ ___ ___ ___ _____    ___  ___ ___ ___  _  _
| _ \ _ \ __| __| __/ __|_   _|  / _ \| _ \_ _/ _ \| \| |
|  _/   / _|| _|| _| (__  | |   | (_) |   /| | (_) | .` |
|_| |_|_\___|_| |___\___| |_|    \___/|_|_\___\___/|_|\_|

Configure Prefect to communicate with the server with:

    prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

View the API reference documentation at http://127.0.0.1:4200/docs

Check out the dashboard at http://127.0.0.1:4200
```

### Googe loud Storage (GCS)

Navigate to your Google clound dashboard and make sure that the project that you created in the last lesson is active. Then click on the button to create a new Storage Bucket.

Give it a name and accept the rest of the default values. If done correctly you should be looking at something like this:

![gcs-bucket](/images/notes/gcs-bucket.png)

> **NOTE:** Make note of the name that you use, you will need to use it in the following script.

### Register the GCP Connection Block in Prefect

Now you will need to register the GCP Block with Prefect in order to be ble to set it up:

```bash
prefect block register -m prefect_gcp
Successfully registered 6 blocks

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Registered Blocks             ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ BigQuery Warehouse            │
│ GCP Cloud Run Job             │
│ GCP Credentials               │
│ GcpSecret                     │
│ GCS Bucket                    │
│ Vertex AI Custom Training Job │
└───────────────────────────────┘

 To configure the newly registered blocks, go to the Blocks page in the Prefect UI: http://127.0.0.1:4200/blocks/catalog
```

### Adding a GCS Bucket

Open up the page indicated and scroll down until you see GSC Bucket and click on the **Add +** button.

![gcs-catalog](/images/notes/gcs-catalog.png)

* Give it a unique **Block Name**
* Enter the name of the **Bucket** that you just created on Google Cloud
* Under **GCP Credentials** click on the **+** sign.

It will now open up a **GCP Credentials** Creation page. Enter a **Block Name**.

![gcp-creds](/images/notes/gcp-creds.png)

Move back to your Google Cloud Dashboard:

* Click on the hmburger
* IAM & Admin
* Service Accounts

![svc-acount](/images/notes/svc-account.png)

Click on **+ CREATE SERVICE ACCOUNT** at the top and give it a **Service account name**.

![svc-acct](/images/notes/svc-acct.png)

Click on the **CREATE AND CONTINUE** button.

Under **Grant this service account access to project** give it the following roles:

* BigQuery Admin
* Storage Admin

![svc-roles](/images/notes/svc-roles.png)

Then click on the **CONTINUE** followed by the **DONE** button.

Once created, click on the new service accouunt link under **email** and select the **KEYS** tab at the top.

![svc-accounts](/images/notes/svc-accounts.png)

* **ADD KEY** drop-down
* Select **Create new key**
* Select the **JSON** option
* Click on the **CREATE** button
* Save the file somewhere save

> **CAUTION:** Do not save it in your repository!

* Copy the contents of the file
* Navigate back to the Orion page
* Paste the contents into the **Service Account Info** field
* Click on the **Create** button.

Back in the **GCS Bucket** Create page, select the credentials that you just created and click on the **Create** button.

You should now be looking at a screen like this. This is the code that you will need to use in order to use your new Bucket.

![dtc-de-gcs](/images/notes/dtc-de-gcs.png)

#### Python script

Let's put this all together into one script:

*etl_web_to_gcs.py:*

```python
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
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
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-de-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)  # type: ignore
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
```

#### Execute the script

Time to run the script:

```python etl_web_to_gcs.py
13:16:15.504 | INFO    | prefect.engine - Created flow run 'diamond-chupacabra' for flow 'etl-web-to-gcs'
13:16:15.668 | INFO    | Flow run 'diamond-chupacabra' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
13:16:15.669 | INFO    | Flow run 'diamond-chupacabra' - Executing 'fetch-b4598a4a-0' immediately...
/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/02_gcp/etl_web_to_gcs.py:11: DtypeWarning: Columns (6) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
13:16:19.672 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
13:16:19.713 | INFO    | Flow run 'diamond-chupacabra' - Created task run 'clean-b9fd7e03-0' for task 'clean'
13:16:19.714 | INFO    | Flow run 'diamond-chupacabra' - Executing 'clean-b9fd7e03-0' immediately...
13:16:20.180 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID tpep_pickup_datetime tpep_dropoff_datetime  ...  improvement_surcharge  total_amount  congestion_surcharge
0       1.0  2021-01-01 00:30:10   2021-01-01 00:36:12  ...                    0.3          11.8                   2.5
1       1.0  2021-01-01 00:51:20   2021-01-01 00:52:19  ...                    0.3           4.3                   0.0

[2 rows x 18 columns]
13:16:20.181 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
13:16:20.182 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 1369765
13:16:20.214 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
13:16:20.252 | INFO    | Flow run 'diamond-chupacabra' - Created task run 'write_local-f322d1be-0' for task 'write_local'
13:16:20.253 | INFO    | Flow run 'diamond-chupacabra' - Executing 'write_local-f322d1be-0' immediately...
13:16:23.484 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
13:16:23.518 | INFO    | Flow run 'diamond-chupacabra' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
13:16:23.518 | INFO    | Flow run 'diamond-chupacabra' - Executing 'write_gcs-1145c921-0' immediately...
13:16:23.626 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'prefect-dtc-de-course'.
13:16:23.931 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-01.parquet') to the bucket 'prefect-dtc-de-course' path 'data/yellow/yellow_tripdata_2021-01.parquet'.
13:16:24.787 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
13:16:24.824 | INFO    | Flow run 'diamond-chupacabra' - Finished in state Completed('All states completed.')
```

### Confirm on GCS Dashboard

Navigate back to your Google dashboard and drill down into your Bucket to confirm that the parquet file was uploaded.

* Google Cloud Dashboard
* Cloud Storage
* Click on your bucket, in my case *prefect-dtc-de-course*
* You should see a `data/` directory
* Inside will be a `yellow/` directory
* Inside will be a `yellow_tripdata_2021-01.parquet` file

![parquet](/images/notes/parquet.png)

If you click on the file, you will get further details on it:

![parquet-details](/images/notes/parquet-details.png)

## ETL from GCS to Big Query

Now that we have our data on GCS, let's deploy it to BQ.

Navigate to your Google Cloud Dashboard and click on the Big Query button.

![dashboard](/images/notes/dashboard.png)

Once in the Big Qeery section, click on the **+ ADD DATA** button at the top. In the *Add Data* screen, click on the **Google Cloud Storage** button.

![add-date](/images/notes/add-data.png)

Source:

* Select a file from the GCS Bucket
  * Click on the **BROWSE** button and navigate to the file that we uploaded before
* File format: **Parquet**

Destination:

* Project: Should be selected
* Dataset: Create a new one and select it
* Table: Give it a name

Then click on **CREATE TABLE** button at the bottom.

![create-table](/images/notes/create-table.png)

You can navigate to the table to confirm that it was created properly.

![rides-table](/images/notes/rides-table.png)

We are going to delete the data in rides for now.

* Query > In new tab

Enter the following command:

```sql
DELETE FROM `dtc-de-course-374214.ny_taxi.rides` WHERE true;
```

![cleared-table](/images/notes/cleared-table.png)

### Python script GCS to BQ

```python
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
```

### Run the script

```bash
python etl_gcs_to_bq.py
10:29:33.214 | INFO    | prefect.engine - Created flow run 'mega-bullfrog' for flow 'etl-gcs-to-bq'
10:29:33.386 | INFO    | Flow run 'mega-bullfrog' - Created task run 'extract_from_gcs-968e3b65-0' for task 'extract_from_gcs'
10:29:33.387 | INFO    | Flow run 'mega-bullfrog' - Executing 'extract_from_gcs-968e3b65-0' immediately...
10:29:34.267 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Downloading blob named data/yellow/yellow_tripdata_2021-01.parquet from the prefect-dtc-de-course bucket to ../data/yellow/data/yellow/yellow_tripdata_2021-01.parquet
10:29:46.634 | INFO    | Task run 'extract_from_gcs-968e3b65-0' - Finished in state Completed()
10:29:46.666 | INFO    | Flow run 'mega-bullfrog' - Created task run 'transform-a7d916b4-0' for task 'transform'
10:29:46.666 | INFO    | Flow run 'mega-bullfrog' - Executing 'transform-a7d916b4-0' immediately...
10:29:47.040 | INFO    | Task run 'transform-a7d916b4-0' - pre: missing passenger count: 98352
10:29:47.045 | INFO    | Task run 'transform-a7d916b4-0' - post: missing passenger count: 0
10:29:47.075 | INFO    | Task run 'transform-a7d916b4-0' - Finished in state Completed()
10:29:47.104 | INFO    | Flow run 'mega-bullfrog' - Created task run 'write_bq-b366772c-0' for task 'write_bq'
10:29:47.104 | INFO    | Flow run 'mega-bullfrog' - Executing 'write_bq-b366772c-0' immediately...
100%|██████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00, 14217.98it/s]
10:30:22.023 | INFO    | Task run 'write_bq-b366772c-0' - Finished in state Completed()
10:30:22.059 | INFO    | Flow run 'mega-bullfrog' - Finished in state Completed('All states completed.')
```

Navigate back to your Google Cloud Dashboard and head back into that table and run the following query:

```sql
SELECT * FROM `dtc-de-course-374214.ny_taxi.rides` LIMIT 1000;
```

The data should now be back.

![rides-data](/images/notes/rides-data.png)

If you run the following query you should get `13697651`:

```sql
SELECT COUNT(*) FROM `dtc-de-course-374214.ny_taxi.rides`;
```

### Parameterize the web to GCS script

Now we are going to modify the script so that we can use parameters and be able to make the script a little more versatile:

```python
from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
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
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-de-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)  # type: ignore
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2],
    year: int = 2021,
    color: str = "yellow",
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)
```

Now we run the script and sit back and watch the magic happen:

```bash
python parameterized_flow.py
12:46:37.157 | INFO    | prefect.engine - Created flow run 'pistachio-cuckoo' for flow 'etl-parent-flow'
12:46:37.382 | INFO    | Flow run 'pistachio-cuckoo' - Created subflow run 'smoky-boa' for flow 'etl-web-to-gcs'
12:46:37.468 | INFO    | Flow run 'smoky-boa' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
12:46:37.469 | INFO    | Flow run 'smoky-boa' - Executing 'fetch-b4598a4a-0' immediately...
/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/03_deployments/etl_web_to_gcs.py:13: DtypeWarning: Columns (6) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
12:46:51.771 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
12:46:51.807 | INFO    | Flow run 'smoky-boa' - Created task run 'clean-b9fd7e03-0' for task 'clean'
12:46:51.808 | INFO    | Flow run 'smoky-boa' - Executing 'clean-b9fd7e03-0' immediately...
12:46:52.284 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID tpep_pickup_datetime tpep_dropoff_datetime  ...  improvement_surcharge  total_amount  congestion_surcharge
0       1.0  2021-01-01 00:30:10   2021-01-01 00:36:12  ...                    0.3          11.8                   2.5
1       1.0  2021-01-01 00:51:20   2021-01-01 00:52:19  ...                    0.3           4.3                   0.0

[2 rows x 18 columns]
12:46:52.286 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
12:46:52.287 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 1369765
12:46:52.318 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
12:46:52.351 | INFO    | Flow run 'smoky-boa' - Created task run 'write_local-f322d1be-0' for task 'write_local'
12:46:52.351 | INFO    | Flow run 'smoky-boa' - Executing 'write_local-f322d1be-0' immediately...
12:46:55.635 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
12:46:55.667 | INFO    | Flow run 'smoky-boa' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
12:46:55.667 | INFO    | Flow run 'smoky-boa' - Executing 'write_gcs-1145c921-0' immediately...
12:46:55.779 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'prefect-dtc-de-course'.
12:46:56.698 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-01.parquet') to the bucket 'prefect-dtc-de-course' path 'data/yellow/yellow_tripdata_2021-01.parquet'.
12:47:06.346 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
12:47:06.389 | INFO    | Flow run 'smoky-boa' - Finished in state Completed('All states completed.')
12:47:06.486 | INFO    | Flow run 'pistachio-cuckoo' - Created subflow run 'inescapable-mackerel' for flow 'etl-web-to-gcs'
12:47:06.567 | INFO    | Flow run 'inescapable-mackerel' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
12:47:06.568 | INFO    | Flow run 'inescapable-mackerel' - Executing 'fetch-b4598a4a-0' immediately...
/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/03_deployments/etl_web_to_gcs.py:13: DtypeWarning: Columns (6) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
12:47:20.922 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
12:47:20.972 | INFO    | Flow run 'inescapable-mackerel' - Created task run 'clean-b9fd7e03-0' for task 'clean'
12:47:20.973 | INFO    | Flow run 'inescapable-mackerel' - Executing 'clean-b9fd7e03-0' immediately...
12:47:21.481 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID tpep_pickup_datetime tpep_dropoff_datetime  ...  improvement_surcharge  total_amount  congestion_surcharge
0       1.0  2021-02-01 00:40:47   2021-02-01 00:48:28  ...                    0.3          12.3                   2.5
1       1.0  2021-02-01 00:07:44   2021-02-01 00:20:31  ...                    0.3          13.3                   0.0

[2 rows x 18 columns]
12:47:21.483 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
12:47:21.484 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 1371708
12:47:21.541 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
12:47:21.576 | INFO    | Flow run 'inescapable-mackerel' - Created task run 'write_local-f322d1be-0' for task 'write_local'
12:47:21.576 | INFO    | Flow run 'inescapable-mackerel' - Executing 'write_local-f322d1be-0' immediately...
12:47:24.978 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
12:47:25.016 | INFO    | Flow run 'inescapable-mackerel' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
12:47:25.016 | INFO    | Flow run 'inescapable-mackerel' - Executing 'write_gcs-1145c921-0' immediately...
12:47:25.132 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'prefect-dtc-de-course'.
12:47:25.726 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-02.parquet') to the bucket 'prefect-dtc-de-course' path 'data/yellow/yellow_tripdata_2021-02.parquet'.
12:47:35.511 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
12:47:35.558 | INFO    | Flow run 'inescapable-mackerel' - Finished in state Completed('All states completed.')
12:47:35.661 | INFO    | Flow run 'pistachio-cuckoo' - Created subflow run 'purring-ant' for flow 'etl-web-to-gcs'
12:47:35.752 | INFO    | Flow run 'purring-ant' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
12:47:35.753 | INFO    | Flow run 'purring-ant' - Executing 'fetch-b4598a4a-0' immediately...
/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/03_deployments/etl_web_to_gcs.py:13: DtypeWarning: Columns (6) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
12:48:02.324 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
12:48:02.623 | INFO    | Flow run 'purring-ant' - Created task run 'clean-b9fd7e03-0' for task 'clean'
12:48:02.624 | INFO    | Flow run 'purring-ant' - Executing 'clean-b9fd7e03-0' immediately...
12:48:03.491 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID tpep_pickup_datetime tpep_dropoff_datetime  ...  improvement_surcharge  total_amount  congestion_surcharge
0       2.0  2021-03-01 00:22:02   2021-03-01 00:23:22  ...                    0.3           4.3                   0.0
1       2.0  2021-03-01 00:24:48   2021-03-01 00:24:56  ...                    0.3           3.8                   0.0

[2 rows x 18 columns]
12:48:03.492 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
tpep_pickup_datetime     datetime64[ns]
tpep_dropoff_datetime    datetime64[ns]
passenger_count                 float64
trip_distance                   float64
RatecodeID                      float64
store_and_fwd_flag               object
PULocationID                      int64
DOLocationID                      int64
payment_type                    float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
improvement_surcharge           float64
total_amount                    float64
congestion_surcharge            float64
dtype: object
12:48:03.493 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 1925152
12:48:03.661 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
12:48:03.997 | INFO    | Flow run 'purring-ant' - Created task run 'write_local-f322d1be-0' for task 'write_local'
12:48:03.998 | INFO    | Flow run 'purring-ant' - Executing 'write_local-f322d1be-0' immediately...
12:48:08.484 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
12:48:08.516 | INFO    | Flow run 'purring-ant' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
12:48:08.517 | INFO    | Flow run 'purring-ant' - Executing 'write_gcs-1145c921-0' immediately...
12:48:08.625 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'prefect-dtc-de-course'.
12:48:08.967 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/yellow/yellow_tripdata_2021-03.parquet') to the bucket 'prefect-dtc-de-course' path 'data/yellow/yellow_tripdata_2021-03.parquet'.
12:48:21.931 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
12:48:21.975 | INFO    | Flow run 'purring-ant' - Finished in state Completed('All states completed.')
12:48:22.010 | INFO    | Flow run 'pistachio-cuckoo' - Finished in state Completed('All states completed.')
```

![param-run](/images/notes/param-run.png)

## Automatic Deployments

We have been manually running the script, but now let's automate it.
There are two ways to create a deployment:

* Command Line Interface (CLI)
* Programmatically througgh Python

### CLI

There are two steps required:

1. Build the deployment definition file `deployment.yml`
2. Create the deployment on the API

#### Build the deployment

Further details can be found in the documentation: [deployment build options](https://docs.prefect.io/concepts/deployments/#deployment-build-options)

The create the deployment definition file, run the following command:

```bash
prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parame
terized ETL"
Found flow 'etl-parent-flow'
Default '.prefectignore' file written to
/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/03_deployments/.prefectig
nore
Deployment YAML created at
'/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/03_deployments/etl_paren
t_flow-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this
warning.
```

This generated an `etl_parent_flow-deployment.yaml` file. In the parameters field, we add the parameters that we used in the Python script:

```yaml
parameters: { "color": "yellow", "months": [1, 2, 3], "year": 2021 }
```

#### Apply the deployment

```bash
prefect deployment apply etl_parent_flow-deployment.yaml
Successfully loaded 'Parameterized ETL'
Deployment 'etl-parent-flow/Parameterized ETL' successfully created with id '6ffebdf8-ca1a-41f3-a175-5640ae3c08d1'.
View Deployment in UI: http://127.0.0.1:4200/deployments/deployment/6ffebdf8-ca1a-41f3-a175-5640ae3c08d1

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

![deployment](/images/notes/deployment.png)

You click on the Description tab to give it a description and view all of the parameters that are set in the Parameters tab.

On the top-right click on **Run** > **Quick run**

If you click on the pop up to view the run, you will see that it's not actually running. That's because we haven't started an agent yet.

Click on **Work Queues**.

![wq](/images/notes/wq.png)

From the **Work Queues** page, click on the `default` queue and copy the command to start the agent and start it:

```bash
prefect agent start  --work-queue "default"
Starting v2.7.10 agent connected to http://127.0.0.1:4200/api...

  ___ ___ ___ ___ ___ ___ _____     _   ___ ___ _  _ _____
 | _ \ _ \ __| __| __/ __|_   _|   /_\ / __| __| \| |_   _|
 |  _/   / _|| _|| _| (__  | |    / _ \ (_ | _|| .` | | |
 |_| |_|_\___|_| |___\___| |_|   /_/ \_\___|___|_|\_| |_|


Agent started! Looking for work from queue(s): default...
13:39:48.571 | INFO    | prefect.agent - Submitting flow run '88fea11f-c2c8-45c4-89e3-ae27ba9ccdeb'
13:39:48.653 | INFO    | prefect.infrastructure.process - Opening process 'warm-capuchin'...
13:39:48.677 | INFO    | prefect.agent - Completed submission of flow run '88fea11f-c2c8-45c4-89e3-ae27ba9ccdeb'
...
```

#### Notifications

You can set up notifications so that you can be alerted to different events. Just navigate over to the **Notifications* page and click on **Create Notification +**.

![notifications](/images/notes/notifications.png)

#### Schedule

If you drill down into your **Deployments**, you will see that you can add a schedule for when you want your deployment to run. Just click on the **Add** button.

You will have the options of:

* Interval
* Cron
* Recurring Rule (RRule)

![schedule](/images/notes/schedule.png)

> **NOTE:** Making changes to **RRule** from the GUI is not supported.

The scheduld job will continue to run as long as **Orion** is up and running and the Deployment has an **Agent** running. You also have the option of toggling it off.

![deployment](/images/notes/deployments.png)

The schedules can also be specified from the command line:

```bash
prefect deployment build parameterized_flow.py:etl_parent_flow -n etl2 --cron "0 0 * * *" -a
Found flow 'etl-parent-flow'
Deployment YAML created at
'/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/03_deployments/etl_parent_fl
ow-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this
warning.
Deployment 'etl-parent-flow/etl2' successfully created with id '9c00b9e4-4238-4ba6-9f7e-f151e5dfa1a1'.

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

![cron-deployment](/images/notes/cron-deployment.png)

### Help

If you ever need to figure out how to do anything, you can just append `--help` to any command to get further details:

```bash
prefect deployment --help
Usage: prefect deployment [OPTIONS] COMMAND [ARGS]...

  Commands for working with deployments.

Options:
  --help  Show this message and exit.

Commands:
  apply            Create or update a deployment from a YAML file.
  build            Generate a deployment YAML from...
  delete           Delete a deployment.
  inspect          View details about a deployment.
  ls               View all deployments or deployments for specific flows.
  pause-schedule   Pause schedule of a given deployment.
  resume-schedule  Resume schedule of a given deployment.
  run              Create a flow run for the given flow and deployment.
  set-schedule     Set schedule for a given deployment.
  ```

As you can see, you can set the schedule after a deployment has been created as well.

## Running flows from Docker containers

### Dockerfile

```docker
FROM prefecthq/prefect:2.7.11-python3.10

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
```

### Docker requirements

```txt
pandas==1.5.2
prefect-gcp[cloud_storage]==0.2.5
protobuf==3.19.6
pyarrow==10.0.1
```

### Create image

```bash
docker image build -t clamytoe/prefect:zoom .
```

### Push to Docker Hub

```bash
docker image push clamytoe/prefect:zoom
The push refers to repository [docker.io/clamytoe/prefect]
88d5442d13fc: Pushed
337834026821: Pushed
2dde53d41073: Pushed
a9532ed06bb8: Mounted from prefecthq/prefect
f68d440d47cb: Mounted from prefecthq/prefect
7d3e0c5b71ce: Mounted from prefecthq/prefect
0dc8e88603a8: Mounted from prefecthq/prefect
00e4d4baf2d2: Mounted from prefecthq/prefect
814b20e3d83d: Mounted from prefecthq/prefect
48e393a194bb: Mounted from prefecthq/prefect
eb7af1182de0: Mounted from prefecthq/prefect
2f28f5507e90: Mounted from prefecthq/prefect
3633b49d846e: Mounted from prefecthq/prefect
7901644b3c3e: Mounted from prefecthq/prefect
03bf8d6415c8: Mounted from prefecthq/prefect
9be38f5f1203: Mounted from prefecthq/prefect
67a4178b7d47: Mounted from prefecthq/prefect
zoom: digest: sha256:ada74d04db5bedaf8ae972045105b4f8a60e2f6db40377b5eab5ced4a6653cae size: 3891
```

![docker-hub](/images/notes/docker-hub.png)

### Docker block

Open up the Orion dashboard and navigate to the Blocks section and create a new Docker Container block:

![docker-container](/images/notes/docker-container.png)

Fill in the following fields:

* Block name: dtc-de-zoom
* Image: clamytoe/prefect:zoom
* ImagePulPolicy: ALWAYS
* Auto Remove: True

Then click on the **CREATE** button.

![dtc-de-zoom](/images/notes/dtc-de-zoom.png)

Copy the code block to use in your deployment:

```bash
from prefect.infrastructure.docker import DockerContainer

docker_container_block = DockerContainer.load("dtc-de-zoom")
```

### Through Python

You can also do the same through Python:

```python
from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="clamytoe/prefect:zoom",
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

docker_block.save("dtc-de-zoom", overwrite=True)
```
