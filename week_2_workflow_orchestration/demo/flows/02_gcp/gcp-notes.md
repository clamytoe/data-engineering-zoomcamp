# ETL with GCP & Prefect

## Start Orion server

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

## Googe loud Storage (GCS)

Navigate to your Google clound dashboard and make sure that the project that you created in the last lesson is active. Then click on the button to create a new Storage Bucket.

Give it a name and accept the rest of the default values. If done correctly you should be looking at something like this:

![gcs-bucket](/images/notes/gcs-bucket.png)

> **NOTE:** Make note of the name that you use, you will need to use it in the following script.

## Register the GCP Connection Block in Prefect

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

## Adding a GCS Bucket

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

## Python script

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

## Execute the script

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

## Confirm on GCS Dashboard

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

## Conclusion

* Created an ETL Flow
* Got data from the web
* Cleaned up data
* Saved it locally
* Uploaded to Google Cloud
