# SQL Statements used for loading data

```sql
-- create external tables
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-374214.trips_data_all.ext_green_tripdata`
OPTIONS (
    format = "PARQUET",
    uris = ["gs://dtc_data_lake_dtc-de-course-374214/dtc_ny_taxi_tripdata/green/*.parquet.snappy"]
);

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-374214.trips_data_all.ext_yellow_tripdata`
OPTIONS (
    format = "PARQUET",
    uris = ["gs://dtc_data_lake_dtc-de-course-374214/dtc_ny_taxi_tripdata/yellow/*.parquet.snappy"]
);

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-374214.trips_data_all.ext_fhv_tripdata`
OPTIONS (
    format = "PARQUET",
    uris = ["gs://dtc_data_lake_dtc-de-course-374214/dtc_ny_taxi_tripdata/fhv/*.parquet.snappy"]
);

-- create native partitioned tables
CREATE OR REPLACE TABLE `dtc-de-course-374214.trips_data_all.green_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
AS (
    SELECT *
    FROM `dtc-de-course-374214.trips_data_all.ext_green_tripdata`
);

CREATE OR REPLACE TABLE `dtc-de-course-374214.trips_data_all.yellow_tripdata`
PARTITION BY DATE(tpep_pickup_datetime)
AS (
    SELECT *
    FROM `dtc-de-course-374214.trips_data_all.ext_yellow_tripdata`
);

CREATE OR REPLACE TABLE `dtc-de-course-374214.trips_data_all.fhv_tripdata`
PARTITION BY DATE(pickup_datetime)
AS (
    SELECT *
    FROM `dtc-de-course-374214.trips_data_all.ext_fhv_tripdata`
);
```
