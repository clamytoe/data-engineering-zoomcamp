# Postgres Docker Commands

## Start Postgres Container

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /home/clamytoe/Projects/data-engineering-zoomcamp/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
  ```

## Connect to Postgres Container

```bash
pgcli -h localhost -p 5432 -u root ny_taxi
```

## Display Tables

```pgcli
root@localhost:ny_taxi> \dt
```

## Confirm Data Upload

```pgcli
root@localhost:ny_taxi> select COUNT(1) from yellow_taxi_data;
+---------+
| count   |
|---------|
| 1369765 |
+---------+
SELECT 1
Time: 0.068s
```

## Run Some Queries

```pgcli
root@localhost:ny_taxi> select max(tpep_pickup_datetime), min(tpep_pickup_datetime), max(total_amount) from yellow_t
 axi_data;
+---------------------+---------------------+---------+
| max                 | min                 | max     |
|---------------------+---------------------+---------|
| 2021-02-22 16:52:16 | 2008-12-31 23:05:14 | 7661.28 |
+---------------------+---------------------+---------+
SELECT 1
Time: 0.092s
```

## Setup and run pgAdmin

Docker Hub: [dpage/pgadmin4](https://hub.docker.com/r/dpage/pgadmin4)
Command: `docker pull dpage/pgadmin4`

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

### Connect to pgAdmin

Bring up a broswer to [localhost:8080](http://localhost:8080) and log in with the credentials that you used.

## Network Setup

The problem with the current setup is that these containers are in their own space and can't talk with each other.
In order to get them to be able to talk, we have to put them in the same network.

### Create the network

```bash
docker network create pg-network
88a830f01dafa6d3fb59a506c5d759e22782a31c49fc7c8350bc1145f734887f
```

### Postgres server

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /home/clamytoe/Projects/data-engineering-zoomcamp/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
  ```

### pgAdmin

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

## Ingest Script

Export your Jupyter Notebook into a python script and dockerize it.

*Dockerfile:*

```docker
FROM python:3.10.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]
```

### Build the image

```bash
docker build -t taxi_ingest:v001 .
```

### Run the image

```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
docker run -it --rm \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}
```

> **NOTE:** For local testing, you can start a local http server and download the data files from your machine: `python -m http.server`

## Docker Compose

Now that everything is working, it's time to make it easier to run by combining all of these steps into one file.

*docker-compose.yaml:*

```yaml
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "/home/clamytoe/Projects/data-engineering-zoomcamp/ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
```

### Run the services

```bash
docker-compose up -d
```

### Stop the services

```bash
docker-compose down
```

## Persistence with pgAdmin

To enable persistence with pgAdmin, make the following changes:

*docker-compose.yaml:*

```yaml
version: "3.9"

services:

  pgdatabase:
    container_name: dtc_postgres
    image: postgres:15
    environment:
      - POSTGRES_DB=ny_taxi
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
    networks:
      - pg-network
    ports:
      - "5432:5432"
    volumes:
      - type: bind
        source: /home/clamytoe/Projects/data-engineering-zoomcamp/srv/ny_taxi_postgres_data
        target: /var/lib/postgresql/data

  pgadmin:
    container_name: dtc_pgadmin
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    networks:
      - pg-network
    ports:
      - "8080:80"
    volumes:
      - type: volume
        source: pgadmin_data
        target: /var/lib/pgadmin

networks:
  pg-network:
    external: true

volumes:
  pgadmin_data:
```
