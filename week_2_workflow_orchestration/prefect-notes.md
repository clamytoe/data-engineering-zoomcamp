# Prefect Notes

## Initial setup

The first thing to do is to create a virtual environment:

```bash
conda create -n zoomcamp python=3.9
conda activate zoomcamp
pip install -r requirements.txt
```

Once the environment has been created, you can confirm that it created successfully with the following command:

```bash
prefect version
Version:             2.7.10
API version:         0.8.4
Python version:      3.10.8
Git commit:          f269d49b
Built:               Thu, Jan 26, 2023 3:51 PM
OS/Arch:             linux/x86_64
Profile:             default
Server type:         ephemeral
Server:
  Database:          sqlite
  SQLite version:    3.40.0
```

## Start the Postgresql and pgAdmin containers

Navigate to last weeks directory and start up the services.

```bash
docker-compose up -d
[+] Running 2/2
 ⠿ Container dtc_pgadmin   Started                                                                                 0.9s
 ⠿ Container dtc_postgres  Started
```

Once the services are running navigate to this week's demo code and execute the `ingest_data.py` script after modifying it to work with your services.

*ingest_data.py*:

```python
#!/usr/bin/env python
# coding: utf-8
# import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_data(user, password, host, port, db, table_name, url):

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")
    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:

        try:
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print("inserted another chunk, took %.3f second" % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == "__main__":
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)
```

Now execute the script:

```bash
python ingest_data.py
...
Connecting to objects.githubusercontent.com (objects.githubusercontent.com)|185.199.109.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 25031880 (24M) [application/octet-stream]
Saving to: ‘yellow_tripdata_2021-01.csv.gz’

yellow_tripdata_2021-01.csv.g 100%[=================================================>]  23.87M  53.6MB/s    in 0.4s

2023-01-31 09:51:10 (53.6 MB/s) - ‘yellow_tripdata_2021-01.csv.gz’ saved [25031880/25031880]

inserted another chunk, took 6.131 second
inserted another chunk, took 6.179 second
inserted another chunk, took 6.503 second
inserted another chunk, took 6.714 second
inserted another chunk, took 6.367 second
inserted another chunk, took 6.260 second
inserted another chunk, took 6.345 second
inserted another chunk, took 6.254 second
inserted another chunk, took 6.266 second
inserted another chunk, took 6.194 second
inserted another chunk, took 6.256 second
inserted another chunk, took 6.192 second
inserted another chunk, took 3.918 second
Finished ingesting data into the postgres database
```

## Confirm data ingestion

Open up your browser to [http://localhost:8080](http://localhost:8080) and log in. Once logged in, open up the Query Tool from *Tools > Query Tool* and issue the following commands:

```sql
select * from yellow_taxi_trips;
```

You will see that the data is indeed in there.

## Use Prefect to automate the ingestion

First we need to drop the table so that we can make sure that it's working. Run the following command in pgAdmin's Query Tool:

```sql
drop table yellow_taxi_trips;
```

Now we will need to modify the `ingest_data.py` script to use prefect:

```python
#!/usr/bin/env python
# coding: utf-8
# import argparse
import os
from time import time

import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine


@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, url):

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")
    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="Ingest Flow")
def main_flow():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)


if __name__ == "__main__":
    main_flow()
```

Once you have your script modified, run it:

```bash
python 01-ingest_data.py
10:21:11.190 | INFO    | prefect.engine - Created flow run 'mahogany-jackrabbit' for flow 'Ingest Flow'
10:21:11.372 | INFO    | Flow run 'mahogany-jackrabbit' - Created task run 'ingest_data-e7246262-0' for task 'ingest_data'
10:21:11.373 | INFO    | Flow run 'mahogany-jackrabbit' - Executing 'ingest_data-e7246262-0' immediately...
Connecting to objects.githubusercontent.com (objects.githubusercontent.com)|185.199.109.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 25031880 (24M) [application/octet-stream]
Saving to: ‘yellow_tripdata_2021-01.csv.gz’

yellow_tripdata_2021-01.csv.g 100%[=================================================>]  23.87M  26.7MB/s    in 0.9s

2023-01-31 10:21:13 (26.7 MB/s) - ‘yellow_tripdata_2021-01.csv.gz’ saved [25031880/25031880]

10:21:19.626 | INFO    | Task run 'ingest_data-e7246262-0' - Finished in state Completed()
10:21:19.656 | INFO    | Flow run 'mahogany-jackrabbit' - Finished in state Completed('All states completed.')
```

You will notice that it now runs as a flow. In our instance its auto-generated name is `mahogany-jackrabbit`.

You can go back to pgAdmin to confirm that the data was indeed ingested.

## Add an ETL pipline

> **NOTE:** Don't forget to drop the table before executing this modified script!

Now we are going to add an ETL pipeline to the script so that we can clean up some of the data. The following changes were made:

```python
python 02-ingest_data.py
10:49:25.668 | INFO    | prefect.engine - Created flow run 'unnatural-caribou' for flow 'Ingest Flow'
10:49:25.844 | INFO    | Flow run 'unnatural-caribou' - Created task run 'extract_data-976b417c-0' for task 'extract_data'
10:49:25.845 | INFO    | Flow run 'unnatural-caribou' - Executing 'extract_data-976b417c-0' immediately...
Connecting to objects.githubusercontent.com (objects.githubusercontent.com)|185.199.111.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 25031880 (24M) [application/octet-stream]
Saving to: ‘yellow_tripdata_2021-01.csv.gz’

yellow_tripdata_2021-01.csv.g 100%[=================================================>]  23.87M  51.7MB/s    in 0.5s

2023-01-31 10:49:26 (51.7 MB/s) - ‘yellow_tripdata_2021-01.csv.gz’ saved [25031880/25031880]

10:49:27.297 | INFO    | Task run 'extract_data-976b417c-0' - Finished in state Completed()
10:49:27.334 | INFO    | Flow run 'unnatural-caribou' - Created task run 'transform_data-7a5e1946-0' for task 'transform_data'
10:49:27.335 | INFO    | Flow run 'unnatural-caribou' - Executing 'transform_data-7a5e1946-0' immediately...
10:49:27.395 | INFO    | Task run 'transform_data-7a5e1946-0' - pre: missing passenger count: 1973
10:49:27.403 | INFO    | Task run 'transform_data-7a5e1946-0' - post: missing passenger count: 0
10:49:27.434 | INFO    | Task run 'transform_data-7a5e1946-0' - Finished in state Completed()
10:49:27.470 | INFO    | Flow run 'unnatural-caribou' - Created task run 'ingest_data-e7246262-0' for task 'ingest_data'
10:49:27.470 | INFO    | Flow run 'unnatural-caribou' - Executing 'ingest_data-e7246262-0' immediately...
10:49:33.842 | INFO    | Task run 'ingest_data-e7246262-0' - Finished in state Completed()
10:49:33.880 | INFO    | Flow run 'unnatural-caribou' - Finished in state Completed('All states completed.')
```

## Parameterization and Sublows

Let's add these:

```python
#!/usr/bin/env python
# coding: utf-8
# import argparse
import os
from datetime import timedelta

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df


@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, df):

    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Flow")
def main_flow(table_name: str):
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)


if __name__ == "__main__":
    main_flow("yellow_taxi_trips")
```

and run it:

```bash
python 03-ingest_data.py
11:25:22.211 | INFO    | prefect.engine - Created flow run 'glittering-snake' for flow 'Ingest Flow'
11:25:22.450 | INFO    | Flow run 'glittering-snake' - Created subflow run 'perfect-mouflon' for flow 'Subflow'
11:25:22.502 | INFO    | Flow run 'perfect-mouflon' - Logging Subflow for: yellow_taxi_trips
11:25:22.540 | INFO    | Flow run 'perfect-mouflon' - Finished in state Completed()
11:25:22.571 | INFO    | Flow run 'glittering-snake' - Created task run 'extract_data-976b417c-0' for task 'extract_data'
11:25:22.572 | INFO    | Flow run 'glittering-snake' - Executing 'extract_data-976b417c-0' immediately...
11:25:22.607 | INFO    | Task run 'extract_data-976b417c-0' - Finished in state Cached(type=COMPLETED)
11:25:22.768 | INFO    | Flow run 'glittering-snake' - Created task run 'transform_data-7a5e1946-0' for task 'transform_data'
11:25:22.769 | INFO    | Flow run 'glittering-snake' - Executing 'transform_data-7a5e1946-0' immediately...
11:25:22.816 | INFO    | Task run 'transform_data-7a5e1946-0' - pre: missing passenger count: 1973
11:25:22.827 | INFO    | Task run 'transform_data-7a5e1946-0' - post: missing passenger count: 0
11:25:22.856 | INFO    | Task run 'transform_data-7a5e1946-0' - Finished in state Completed()
11:25:22.892 | INFO    | Flow run 'glittering-snake' - Created task run 'ingest_data-e7246262-0' for task 'ingest_data'
11:25:22.892 | INFO    | Flow run 'glittering-snake' - Executing 'ingest_data-e7246262-0' immediately...
11:25:29.440 | INFO    | Task run 'ingest_data-e7246262-0' - Finished in state Completed()
11:25:29.473 | INFO    | Flow run 'glittering-snake' - Finished in state Completed('All states completed.')
```

## Orion

We can see all of the flow that we have alrady started:

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

> **NOTE:** Make sure to run the command indicated if you haven't!

```bash
 prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
Set 'PREFECT_API_URL' to 'http://127.0.0.1:4200/api'.
Updated profile 'default'.
```

![orion](/images/notes/orion.png)

![flow](/images/notes/flow.png)

## Blocks

Blocks are permanent storage configurations.

There are many installable colections. You can browser them all here: [Collections Catalog](https://docs.prefect.io/collections/catalog/)

![collections](/images/notes/collections.png)

We are going to setup a `SQLAlchemy` one, so click on that one. Here you will find instructions on how to add it and use it.

Back on the Orion page, click on **Blocks** on the left menu and click on the **Add Block +** button and navigate down until you find the **SQLAlchemy Connector** one and click on the **Add +** button.

![sqlalchemy](/images/notes/sqlalchemy.png)

Make the following selections/entries:

* Block Name: postgres-connector
* Driver: SyncDriver (postgresql+psycopg2)
* Database: ny_taxi
* Username: root
* Password: root
* Host: localhost
* Post: 5432

Once created it should look like this:

![connector](/images/notes/connector.png)

## SQLAlchemy Connector

Modify the script as follows in order to use the SQLAlchemy Connector:

```python
#!/usr/bin/env python
# coding: utf-8
# import argparse
import os
from datetime import timedelta

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import create_engine


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    conection_block = SqlAlchemyConnector.load("postgres-connector")

    with conection_block.get_connection(begin=False) as engine:  # type: ignore
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        df.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Flow")
def main_flow(table_name: str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)


if __name__ == "__main__":
    main_flow("yellow_taxi_trips")
```

and run the script:

```bash
python 04-ingest_data.py
13:26:55.068 | INFO    | prefect.engine - Created flow run 'adventurous-hummingbird' for flow 'Ingest Flow'
13:26:55.284 | INFO    | Flow run 'adventurous-hummingbird' - Created subflow run 'lively-goose' for flow 'Subflow'
13:26:55.334 | INFO    | Flow run 'lively-goose' - Logging Subflow for: yellow_taxi_trips
13:26:55.376 | INFO    | Flow run 'lively-goose' - Finished in state Completed()
13:26:55.410 | INFO    | Flow run 'adventurous-hummingbird' - Created task run 'extract_data-976b417c-0' for task 'extract_data'
13:26:55.411 | INFO    | Flow run 'adventurous-hummingbird' - Executing 'extract_data-976b417c-0' immediately...
13:26:55.445 | INFO    | Task run 'extract_data-976b417c-0' - Finished in state Cached(type=COMPLETED)
13:26:55.644 | INFO    | Flow run 'adventurous-hummingbird' - Created task run 'transform_data-7a5e1946-0' for task 'transform_data'
13:26:55.644 | INFO    | Flow run 'adventurous-hummingbird' - Executing 'transform_data-7a5e1946-0' immediately...
13:26:55.693 | INFO    | Task run 'transform_data-7a5e1946-0' - pre: missing passenger count: 1973
13:26:55.703 | INFO    | Task run 'transform_data-7a5e1946-0' - post: missing passenger count: 0
13:26:55.735 | INFO    | Task run 'transform_data-7a5e1946-0' - Finished in state Completed()
13:26:55.768 | INFO    | Flow run 'adventurous-hummingbird' - Created task run 'ingest_data-e7246262-0' for task 'ingest_data'
13:26:55.769 | INFO    | Flow run 'adventurous-hummingbird' - Executing 'ingest_data-e7246262-0' immediately...
13:26:55.848 | INFO    | Task run 'ingest_data-e7246262-0' - Created a new engine.
13:26:55.872 | INFO    | Task run 'ingest_data-e7246262-0' - Created a new connection.
13:27:02.356 | INFO    | Task run 'ingest_data-e7246262-0' - Finished in state Completed()
13:27:02.392 | INFO    | Flow run 'adventurous-hummingbird' - Finished in state Completed('All states completed.')
```
