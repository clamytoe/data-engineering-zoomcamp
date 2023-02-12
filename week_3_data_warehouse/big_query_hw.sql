CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-374214.ny_taxi.fhv_external_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-dtc-de-course/data/fhv/fhv_external_data_2019-01.csv.gz']
);


SELECT count(*) FROM `dtc-de-course-374214.ny_taxi.fhv_external_data`;


SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `dtc-de-course-374214.ny_taxi.fhv_external_data`;

SELECT
    COUNT(1)
FROM
    `dtc-de-course-374214.ny_taxi.fhv_external_data` fhv
WHERE
    fhv.PUlocationID IS NULL
    AND fhv.DOlocationID IS NULL;

CREATE OR REPLACE TABLE `dtc-de-course-374214.ny_taxi.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `dtc-de-course-374214.ny_taxi.fhv_external_data`;

CREATE OR REPLACE TABLE `dtc-de-course-374214.ny_taxi.fhv_partitioned_tripdata`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `dtc-de-course-374214.ny_taxi.fhv_external_data`
);

SELECT distinct affiliated_base_number FROM  `dtc-de-course-374214.ny_taxi.fhv_nonpartitioned_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';


SELECT distinct affiliated_base_number FROM `dtc-de-course-374214.ny_taxi.fhv_partitioned_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-374214.ny_taxi.fhv_external_parquet_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://prefect-dtc-de-course/data/fhv/fhv_tripdata_2019-*.parquet.gz']
);

-- 43244696
SELECT count(1) FROM `dtc-de-course-374214.ny_taxi.fhv_external_parquet_data`;

-- 43244696
SELECT count(1) FROM `dtc-de-course-374214.ny_taxi.fhv_external_data`;