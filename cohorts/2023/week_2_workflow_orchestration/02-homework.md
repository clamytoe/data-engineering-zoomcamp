# Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation.

## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

```bash
python etl_web_to_gcs_hw.py
13:53:59.964 | INFO    | prefect.engine - Created flow run 'super-dalmatian' for flow 'etl-web-to-gcs'
13:54:00.119 | INFO    | Flow run 'super-dalmatian' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
13:54:00.120 | INFO    | Flow run 'super-dalmatian' - Executing 'fetch-b4598a4a-0' immediately...
/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/02_gcp/etl_web_to_gcs_hw.py:11: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
13:54:01.567 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
13:54:01.599 | INFO    | Flow run 'super-dalmatian' - Created task run 'clean-b9fd7e03-0' for task 'clean'
13:54:01.600 | INFO    | Flow run 'super-dalmatian' - Executing 'clean-b9fd7e03-0' immediately...
13:54:01.791 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID lpep_pickup_datetime lpep_dropoff_datetime  ... payment_type  trip_type  congestion_surcharge
0       2.0  2019-12-18 15:52:30   2019-12-18 15:54:39  ...          1.0        1.0                   0.0
1       2.0  2020-01-01 00:45:58   2020-01-01 00:56:39  ...          1.0        2.0                   0.0

[2 rows x 20 columns]
13:54:01.792 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
13:54:01.793 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
13:54:01.824 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
13:54:01.858 | INFO    | Flow run 'super-dalmatian' - Created task run 'write_local-f322d1be-0' for task 'write_local'
13:54:01.859 | INFO    | Flow run 'super-dalmatian' - Executing 'write_local-f322d1be-0' immediately...
13:54:02.859 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
13:54:02.890 | INFO    | Flow run 'super-dalmatian' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
13:54:02.891 | INFO    | Flow run 'super-dalmatian' - Executing 'write_gcs-1145c921-0' immediately...
13:54:03.001 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'prefect-dtc-de-course'.
13:54:03.251 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/green/green_tripdata_2020-01.parquet') to the bucket 'prefect-dtc-de-course' path 'data/green/green_tripdata_2020-01.parquet'.
13:54:03.723 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
13:54:03.757 | INFO    | Flow run 'super-dalmatian' - Finished in state Completed('All states completed.')
```

How many rows does that dataset have?

* [X] **447,770**
* [ ] 766,792
* [ ] 299,234
* [ ] 822,132

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows.

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

```bash
prefect deployment build etl_web_to_gcs_hw.py:etl_web_to_gcs -n monthly_etl --cron "0 5 1 * *
" -a
Found flow 'etl-web-to-gcs'
Default '.prefectignore' file written to
/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/02_gcp/.prefectignore
Deployment YAML created at
'/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/02_gcp/etl_web_to_gcs-deploy
ment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this
warning.
Deployment 'etl-web-to-gcs/monthly_etl' successfully created with id 'e411636d-2e1a-471a-8ed7-6136455106e8'.

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

![cron](cron.png)

* [X] **`0 5 1 * *`**
* [ ] `0 0 5 1 *`
* [ ] `5 * 1 0 *`
* [ ] `* * 5 1 0`

## Question 3. Loading data to BigQuery

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

```python
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")  # type: ignore
    return Path(f"../data/{gcs_path}")


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="prefect-sbx-community-eng",
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
```

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

```bash
prefect deployment build ./etl_gcs_to_bq_hw.py:etl_gcs_to_bq -n "GCS to BQ Parameterized"
Found flow 'etl-gcs-to-bq'
Deployment YAML created at
'/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/02_gcp/etl_gcs_to_bq-deploym
ent.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this
warning.
```

*parameters:*

```yaml
parameters: { "months": [2, 3], "color": "yellow", "year": 2019 }
```

```bash
prefect deployment apply etl_gcs_to_bq-deployment.yaml
Successfully loaded 'GCS to BQ Parameterized'
Deployment 'etl-gcs-to-bq/GCS to BQ Parameterized' successfully created with id 'c3188b0b-975c-41a7-ac09-7123fd7b5929'.
View Deployment in UI: http://127.0.0.1:4200/deployments/deployment/c3188b0b-975c-41a7-ac09-7123fd7b5929

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

![gcs-bq-dep](gcs-bq-dep.png)

*start agent:*

```bash
prefect agent start -q 'default'
Starting v2.7.10 agent connected to http://127.0.0.1:4200/api...

  ___ ___ ___ ___ ___ ___ _____     _   ___ ___ _  _ _____
 | _ \ _ \ __| __| __/ __|_   _|   /_\ / __| __| \| |_   _|
 |  _/   / _|| _|| _| (__  | |    / _ \ (_ | _|| .` | | |
 |_| |_|_\___|_| |___\___| |_|   /_/ \_\___|___|_|\_| |_|


Agent started! Looking for work from queue(s): default...
```

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS.

![gcs-2019](gcs-2019.png)

Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

```bash
15:46:01.726 | INFO    | prefect.agent - Submitting flow run '6b681f5d-b1d6-4c38-82a7-9525dc49ba01'
15:46:01.798 | INFO    | prefect.infrastructure.process - Opening process 'esoteric-serval'...
15:46:01.820 | INFO    | prefect.agent - Completed submission of flow run '6b681f5d-b1d6-4c38-82a7-9525dc49ba01'
/home/clamytoe/miniconda3/envs/de/lib/python3.10/runpy.py:126: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
15:46:03.652 | INFO    | Flow run 'esoteric-serval' - Downloading flow code from storage at '/home/clamytoe/Projects/data-engineering-zoomcamp/week_2_workflow_orchestration/demo/flows/02_gcp'
15:46:03.979 | INFO    | Flow run 'esoteric-serval' - Created task run 'extract_from_gcs-272ec809-0' for task 'extract_from_gcs'
15:46:03.980 | INFO    | Flow run 'esoteric-serval' - Executing 'extract_from_gcs-272ec809-0' immediately...
15:46:04.335 | INFO    | Task run 'extract_from_gcs-272ec809-0' - Downloading blob named data/yellow/yellow_tripdata_2019-02.parquet from the prefect-dtc-de-course bucket to ../data/data/yellow/yellow_tripdata_2019-02.parquet
15:46:07.339 | INFO    | Task run 'extract_from_gcs-272ec809-0' - Finished in state Completed()
15:46:07.824 | INFO    | Flow run 'esoteric-serval' - Created task run 'write_bq-f3b17cf5-0' for task 'write_bq'
15:46:07.825 | INFO    | Flow run 'esoteric-serval' - Executing 'write_bq-f3b17cf5-0' immediately...
15:47:00.391 | INFO    | Task run 'write_bq-f3b17cf5-0' - Finished in state Completed()
15:47:00.425 | INFO    | Flow run 'esoteric-serval' - Created task run 'extract_from_gcs-272ec809-1' for task 'extract_from_gcs'
15:47:00.426 | INFO    | Flow run 'esoteric-serval' - Executing 'extract_from_gcs-272ec809-1' immediately...
15:47:00.961 | INFO    | Task run 'extract_from_gcs-272ec809-1' - Downloading blob named data/yellow/yellow_tripdata_2019-03.parquet from the prefect-dtc-de-course bucket to ../data/data/yellow/yellow_tripdata_2019-03.parquet
15:47:04.456 | INFO    | Task run 'extract_from_gcs-272ec809-1' - Finished in state Completed()
15:47:05.154 | INFO    | Flow run 'esoteric-serval' - Created task run 'write_bq-f3b17cf5-1' for task 'write_bq'
15:47:05.155 | INFO    | Flow run 'esoteric-serval' - Executing 'write_bq-f3b17cf5-1' immediately...
15:48:29.761 | INFO    | Task run 'write_bq-f3b17cf5-1' - Finished in state Completed()
15:48:29.762 | INFO    | Flow run 'esoteric-serval' - Total number of rows processed: 14851920
15:48:29.799 | INFO    | Flow run 'esoteric-serval' - Finished in state Completed('All states completed.')
15:48:30.256 | INFO    | prefect.infrastructure.process - Process 'esoteric-serval' exited cleanly.
```

* [X] **14,851,920**
* [ ] 12,282,990
* [ ] 27,235,753
* [ ] 11,338,483

## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.

![gh-block](gh-block.png)

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

```bash
prefect deployment build week_2_workflow_orchestration/demo/flows/02_gcp/gh_flows/etl_web_to_gh_hw.py:etl_web_to_gcs --name git_flow -sb github/clamytoe-dez -a
Found flow 'etl-web-to-gcs'
Default '.prefectignore' file written to /home/clamytoe/Projects/data-engineering-zoomcamp/.prefectignore
Deployment YAML created at '/home/clamytoe/Projects/data-engineering-zoomcamp/etl_web_to_gcs-deployment.yaml'.
Deployment storage GitHub(repository='https://github.com/clamytoe/data-engineering-zoomcamp', reference=None,
access_token=None, include_git_objects=True) does not have upload capabilities; no files uploaded.  Pass --skip-upload
to suppress this warning.
Deployment 'etl-web-to-gcs/git_flow' successfully created with id 'f295ebd9-efef-421f-8b12-57e846bd0491'.

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

![gh-dep](gh-dep.png)

```bash
prefect agent start -q 'default'
Starting v2.7.10 agent connected to http://127.0.0.1:4200/api...

  ___ ___ ___ ___ ___ ___ _____     _   ___ ___ _  _ _____
 | _ \ _ \ __| __| __/ __|_   _|   /_\ / __| __| \| |_   _|
 |  _/   / _|| _|| _| (__  | |    / _ \ (_ | _|| .` | | |
 |_| |_|_\___|_| |___\___| |_|   /_/ \_\___|___|_|\_| |_|


Agent started! Looking for work from queue(s): default...
18:56:49.692 | INFO    | prefect.agent - Submitting flow run '523bd006-9e89-407b-9864-c5e228072766'
18:56:49.773 | INFO    | prefect.infrastructure.process - Opening process 'denim-ferret'...
18:56:49.796 | INFO    | prefect.agent - Completed submission of flow run '523bd006-9e89-407b-9864-c5e228072766'
/home/clamytoe/miniconda3/envs/de/lib/python3.10/runpy.py:126: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
18:56:51.810 | INFO    | Flow run 'denim-ferret' - Downloading flow code from storage at ''
18:56:53.502 | INFO    | Flow run 'denim-ferret' - Created task run 'fetch-ba00c645-0' for task 'fetch'
18:56:53.503 | INFO    | Flow run 'denim-ferret' - Executing 'fetch-ba00c645-0' immediately...
/tmp/tmpo1lse_3iprefect/week_2_workflow_orchestration/demo/flows/02_gcp/gh_flows/etl_web_to_gh_hw.py:11: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
18:56:54.484 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Completed()
18:56:54.519 | INFO    | Flow run 'denim-ferret' - Created task run 'clean-2c6af9f6-0' for task 'clean'
18:56:54.519 | INFO    | Flow run 'denim-ferret' - Executing 'clean-2c6af9f6-0' immediately...
18:56:54.623 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
0       2.0  2020-11-01 00:08:23  ...       1.0                 2.75
1       2.0  2020-11-01 00:23:32  ...       1.0                 0.00

[2 rows x 20 columns]
18:56:54.624 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
18:56:54.625 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605
18:56:54.657 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
18:56:54.701 | INFO    | Flow run 'denim-ferret' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
18:56:54.701 | INFO    | Flow run 'denim-ferret' - Executing 'write_local-09e9d2b8-0' immediately...
18:56:55.004 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
18:56:55.037 | INFO    | Flow run 'denim-ferret' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
18:56:55.038 | INFO    | Flow run 'denim-ferret' - Executing 'write_gcs-67f8f48e-0' immediately...
18:56:55.149 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'prefect-dtc-de-course'.
18:56:56.078 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('data/green/green_tripdata_2020-11.parquet') to the bucket 'prefect-dtc-de-course' path 'data/green/green_tripdata_2020-11.parquet'.
18:56:56.550 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
18:56:56.588 | INFO    | Flow run 'denim-ferret' - Finished in state Completed('All states completed.')
18:56:56.985 | INFO    | prefect.infrastructure.process - Process 'denim-ferret' exited cleanly.
```

How many rows were processed by the script?

* [ ] 88,019
* [ ] 192,297
* [X] **88,605**
* [ ] 190,225

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.

Set up an Automation that will send yourself an email when a flow run completes.

Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days.

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: <https://hooks.slack.com/services/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>

![slack-notification](slack-notification.png)

Test the functionality.

![bot-test](bot-test.png)

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create.

![mohhsbot](mohhsbot.png)

How many rows were processed by the script?

* [ ] `125,268`
* [ ] `377,922`
* [ ] `728,390`
* [X] **`514,392`**

## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

![secretos](secretos.png)

* [ ] 5
* [ ] 6
* [X] **8**
* [ ] 10

## Submitting the solutions

* Form for submitting: <https://forms.gle/PY8mBEGXJ1RvmTM97>
* You can submit your homework multiple times. In this case, only the last submission will be used.

Deadline: 6 February (Monday), 22:00 CET

## Solution

We will publish the solution here
