# Spark Notes

## Processing Data

* Batch 80%
* Streaming 20%

### Batch Jobs

* Weekly
* Daily
* Hourly
* Odometer per hour
* Every 5 minutes

### Technologies

* Python scripts
* SQL
* Spark
* FLINK

### Workflow

* Datalake (CSV)
* Python
* SQL (DBT)
* Spark
* Python

### Advantages of Batch Jobs

* Easy to manage
* Retry
* Scalable

### Disadvantages

* Delay

## Apache Spark

Apache Spark is an open-source unified analytics engine for large-scale data processing. Spark provides an interface for programming clusters with implicit data parallelism and fault tolerance.

* Data processing engine
* Multi-language
  * Java
  * Scala
  * Python (pyspark)
  * R
* Used for:
  * executing batch jobs
  * streaming

### When to use Spark?

* Data in Data Lake Parquet (S3/GCS)
* SQL (Spark/Hive/Presto/Athena)
* Data Lake

> If you cannot express your batch job as SQL, use Spark

## Download Taxi Datasets

Use my download_datasets.py script to download the dataset files. Once successfully ran, you should have the following directory structure:

```bash
data
└── raw
    ├── fhv
    │   ├── 2019
    │   │   ├── 01
    │   │   │   ├── fhv_tripdata_2019-01.csv.gz
    │   │   │   └── fhv_tripdata_2019-01.csv.gzak892sfl.tmp
    │   │   ├── 02
    │   │   │   └── fhv_tripdata_2019-02.csv.gz
    │   │   ├── 03
    │   │   │   └── fhv_tripdata_2019-03.csv.gz
    │   │   ├── 04
    │   │   │   └── fhv_tripdata_2019-04.csv.gz
    │   │   ├── 05
    │   │   │   └── fhv_tripdata_2019-05.csv.gz
    │   │   ├── 06
    │   │   │   └── fhv_tripdata_2019-06.csv.gz
    │   │   ├── 07
    │   │   │   └── fhv_tripdata_2019-07.csv.gz
    │   │   ├── 08
    │   │   │   └── fhv_tripdata_2019-08.csv.gz
    │   │   ├── 09
    │   │   │   └── fhv_tripdata_2019-09.csv.gz
    │   │   ├── 10
    │   │   │   └── fhv_tripdata_2019-10.csv.gz
    │   │   ├── 11
    │   │   │   └── fhv_tripdata_2019-11.csv.gz
    │   │   └── 12
    │   │       └── fhv_tripdata_2019-12.csv.gz
    │   ├── 2020
    │   │   ├── 01
    │   │   │   └── fhv_tripdata_2020-01.csv.gz
    │   │   ├── 02
    │   │   │   └── fhv_tripdata_2020-02.csv.gz
    │   │   ├── 03
    │   │   │   └── fhv_tripdata_2020-03.csv.gz
    │   │   ├── 04
    │   │   │   └── fhv_tripdata_2020-04.csv.gz
    │   │   ├── 05
    │   │   │   └── fhv_tripdata_2020-05.csv.gz
    │   │   ├── 06
    │   │   │   └── fhv_tripdata_2020-06.csv.gz
    │   │   ├── 07
    │   │   │   └── fhv_tripdata_2020-07.csv.gz
    │   │   ├── 08
    │   │   │   └── fhv_tripdata_2020-08.csv.gz
    │   │   ├── 09
    │   │   │   └── fhv_tripdata_2020-09.csv.gz
    │   │   ├── 10
    │   │   │   └── fhv_tripdata_2020-10.csv.gz
    │   │   ├── 11
    │   │   │   └── fhv_tripdata_2020-11.csv.gz
    │   │   └── 12
    │   │       └── fhv_tripdata_2020-12.csv.gz
    │   └── 2021
    │       ├── 01
    │       │   └── fhv_tripdata_2021-01.csv.gz
    │       ├── 02
    │       │   └── fhv_tripdata_2021-02.csv.gz
    │       ├── 03
    │       │   └── fhv_tripdata_2021-03.csv.gz
    │       ├── 04
    │       │   └── fhv_tripdata_2021-04.csv.gz
    │       ├── 05
    │       │   └── fhv_tripdata_2021-05.csv.gz
    │       ├── 06
    │       │   └── fhv_tripdata_2021-06.csv.gz
    │       └── 07
    │           └── fhv_tripdata_2021-07.csv.gz
    ├── green
    │   ├── 2019
    │   │   ├── 01
    │   │   │   └── green_tripdata_2019-01.csv.gz
    │   │   ├── 02
    │   │   │   └── green_tripdata_2019-02.csv.gz
    │   │   ├── 03
    │   │   │   └── green_tripdata_2019-03.csv.gz
    │   │   ├── 04
    │   │   │   └── green_tripdata_2019-04.csv.gz
    │   │   ├── 05
    │   │   │   └── green_tripdata_2019-05.csv.gz
    │   │   ├── 06
    │   │   │   └── green_tripdata_2019-06.csv.gz
    │   │   ├── 07
    │   │   │   └── green_tripdata_2019-07.csv.gz
    │   │   ├── 08
    │   │   │   └── green_tripdata_2019-08.csv.gz
    │   │   ├── 09
    │   │   │   └── green_tripdata_2019-09.csv.gz
    │   │   ├── 10
    │   │   │   └── green_tripdata_2019-10.csv.gz
    │   │   ├── 11
    │   │   │   └── green_tripdata_2019-11.csv.gz
    │   │   └── 12
    │   │       └── green_tripdata_2019-12.csv.gz
    │   ├── 2020
    │   │   ├── 01
    │   │   │   └── green_tripdata_2020-01.csv.gz
    │   │   ├── 02
    │   │   │   └── green_tripdata_2020-02.csv.gz
    │   │   ├── 03
    │   │   │   └── green_tripdata_2020-03.csv.gz
    │   │   ├── 04
    │   │   │   └── green_tripdata_2020-04.csv.gz
    │   │   ├── 05
    │   │   │   └── green_tripdata_2020-05.csv.gz
    │   │   ├── 06
    │   │   │   └── green_tripdata_2020-06.csv.gz
    │   │   ├── 07
    │   │   │   └── green_tripdata_2020-07.csv.gz
    │   │   ├── 08
    │   │   │   └── green_tripdata_2020-08.csv.gz
    │   │   ├── 09
    │   │   │   └── green_tripdata_2020-09.csv.gz
    │   │   ├── 10
    │   │   │   └── green_tripdata_2020-10.csv.gz
    │   │   ├── 11
    │   │   │   └── green_tripdata_2020-11.csv.gz
    │   │   └── 12
    │   │       └── green_tripdata_2020-12.csv.gz
    │   └── 2021
    │       ├── 01
    │       │   └── green_tripdata_2021-01.csv.gz
    │       ├── 02
    │       │   └── green_tripdata_2021-02.csv.gz
    │       ├── 03
    │       │   └── green_tripdata_2021-03.csv.gz
    │       ├── 04
    │       │   └── green_tripdata_2021-04.csv.gz
    │       ├── 05
    │       │   └── green_tripdata_2021-05.csv.gz
    │       ├── 06
    │       │   └── green_tripdata_2021-06.csv.gz
    │       └── 07
    │           └── green_tripdata_2021-07.csv.gz
    └── yellow
        ├── 2019
        │   ├── 01
        │   │   └── yellow_tripdata_2019-01.csv.gz
        │   ├── 02
        │   │   └── yellow_tripdata_2019-02.csv.gz
        │   ├── 03
        │   │   └── yellow_tripdata_2019-03.csv.gz
        │   ├── 04
        │   │   └── yellow_tripdata_2019-04.csv.gz
        │   ├── 05
        │   │   └── yellow_tripdata_2019-05.csv.gz
        │   ├── 06
        │   │   └── yellow_tripdata_2019-06.csv.gz
        │   ├── 07
        │   │   └── yellow_tripdata_2019-07.csv.gz
        │   ├── 08
        │   │   └── yellow_tripdata_2019-08.csv.gz
        │   ├── 09
        │   │   └── yellow_tripdata_2019-09.csv.gz
        │   ├── 10
        │   │   └── yellow_tripdata_2019-10.csv.gz
        │   ├── 11
        │   │   └── yellow_tripdata_2019-11.csv.gz
        │   └── 12
        │       └── yellow_tripdata_2019-12.csv.gz
        ├── 2020
        │   ├── 01
        │   │   └── yellow_tripdata_2020-01.csv.gz
        │   ├── 02
        │   │   └── yellow_tripdata_2020-02.csv.gz
        │   ├── 03
        │   │   └── yellow_tripdata_2020-03.csv.gz
        │   ├── 04
        │   │   └── yellow_tripdata_2020-04.csv.gz
        │   ├── 05
        │   │   └── yellow_tripdata_2020-05.csv.gz
        │   ├── 06
        │   │   └── yellow_tripdata_2020-06.csv.gz
        │   ├── 07
        │   │   └── yellow_tripdata_2020-07.csv.gz
        │   ├── 08
        │   │   └── yellow_tripdata_2020-08.csv.gz
        │   ├── 09
        │   │   └── yellow_tripdata_2020-09.csv.gz
        │   ├── 10
        │   │   └── yellow_tripdata_2020-10.csv.gz
        │   ├── 11
        │   │   └── yellow_tripdata_2020-11.csv.gz
        │   └── 12
        │       └── yellow_tripdata_2020-12.csv.gz
        └── 2021
            ├── 01
            │   └── yellow_tripdata_2021-01.csv.gz
            ├── 02
            │   └── yellow_tripdata_2021-02.csv.gz
            ├── 03
            │   └── yellow_tripdata_2021-03.csv.gz
            ├── 04
            │   └── yellow_tripdata_2021-04.csv.gz
            ├── 05
            │   └── yellow_tripdata_2021-05.csv.gz
            ├── 06
            │   └── yellow_tripdata_2021-06.csv.gz
            └── 07
                └── yellow_tripdata_2021-07.csv.gz
```

## Adjust the schema and save as parquet

For full details on how to create a schema for spark look in: `03_taxi_schema.ipynb`.

```python
df_fhv_pd = pd.read_csv("data/raw/fhv/2021/01/fhv_tripdata_2021-01.csv.gz", nrows=1000)
spark.createDataFrame(df_fhv_pd).schema
```

You take the output and modify the schema to better optimize the features:

```python
fhv_schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.BooleanType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True),
])
```

Then you use the schema to generate your parquet files:

```python
year = 2020

for month in range(1, 13):
    print(f'processing data for {year}/{month}')

    input_path = f'data/raw/fhv/{year}/{month:02d}/'
    output_path = f'data/pq/fhv/{year}/{month:02d}/'

    df_yellow = spark.read \
        .option("header", "true") \
        .schema(fhv_schema) \
        .csv(input_path)

    df_yellow \
        .repartition(6) \
        .write.parquet(output_path)
```

```bash
tree data/pq/fhv/2020
data/pq/fhv/2020
├── 01
│   ├── _SUCCESS
│   ├── part-00000-e15c0295-0a90-4e78-b315-237b6e9c1fbd-c000.snappy.parquet
│   ├── part-00001-e15c0295-0a90-4e78-b315-237b6e9c1fbd-c000.snappy.parquet
│   ├── part-00002-e15c0295-0a90-4e78-b315-237b6e9c1fbd-c000.snappy.parquet
│   ├── part-00003-e15c0295-0a90-4e78-b315-237b6e9c1fbd-c000.snappy.parquet
│   ├── part-00004-e15c0295-0a90-4e78-b315-237b6e9c1fbd-c000.snappy.parquet
│   └── part-00005-e15c0295-0a90-4e78-b315-237b6e9c1fbd-c000.snappy.parquet
├── 02
│   ├── _SUCCESS
│   ├── part-00000-c0718c50-3f07-4dec-9c3f-c722c4367620-c000.snappy.parquet
│   ├── part-00001-c0718c50-3f07-4dec-9c3f-c722c4367620-c000.snappy.parquet
│   ├── part-00002-c0718c50-3f07-4dec-9c3f-c722c4367620-c000.snappy.parquet
│   ├── part-00003-c0718c50-3f07-4dec-9c3f-c722c4367620-c000.snappy.parquet
│   ├── part-00004-c0718c50-3f07-4dec-9c3f-c722c4367620-c000.snappy.parquet
│   └── part-00005-c0718c50-3f07-4dec-9c3f-c722c4367620-c000.snappy.parquet
├── 03
│   ├── _SUCCESS
│   ├── part-00000-985c259e-7c0c-44c4-b207-5efaf8236b8d-c000.snappy.parquet
│   ├── part-00001-985c259e-7c0c-44c4-b207-5efaf8236b8d-c000.snappy.parquet
│   ├── part-00002-985c259e-7c0c-44c4-b207-5efaf8236b8d-c000.snappy.parquet
│   ├── part-00003-985c259e-7c0c-44c4-b207-5efaf8236b8d-c000.snappy.parquet
│   ├── part-00004-985c259e-7c0c-44c4-b207-5efaf8236b8d-c000.snappy.parquet
│   └── part-00005-985c259e-7c0c-44c4-b207-5efaf8236b8d-c000.snappy.parquet
├── 04
│   ├── _SUCCESS
│   ├── part-00000-6e38cf03-5415-455d-9664-e6782399e8de-c000.snappy.parquet
│   ├── part-00001-6e38cf03-5415-455d-9664-e6782399e8de-c000.snappy.parquet
│   ├── part-00002-6e38cf03-5415-455d-9664-e6782399e8de-c000.snappy.parquet
│   ├── part-00003-6e38cf03-5415-455d-9664-e6782399e8de-c000.snappy.parquet
│   ├── part-00004-6e38cf03-5415-455d-9664-e6782399e8de-c000.snappy.parquet
│   └── part-00005-6e38cf03-5415-455d-9664-e6782399e8de-c000.snappy.parquet
├── 05
│   ├── _SUCCESS
│   ├── part-00000-0c9471b3-9da8-467b-88ca-2c698c9b381f-c000.snappy.parquet
│   ├── part-00001-0c9471b3-9da8-467b-88ca-2c698c9b381f-c000.snappy.parquet
│   ├── part-00002-0c9471b3-9da8-467b-88ca-2c698c9b381f-c000.snappy.parquet
│   ├── part-00003-0c9471b3-9da8-467b-88ca-2c698c9b381f-c000.snappy.parquet
│   ├── part-00004-0c9471b3-9da8-467b-88ca-2c698c9b381f-c000.snappy.parquet
│   └── part-00005-0c9471b3-9da8-467b-88ca-2c698c9b381f-c000.snappy.parquet
├── 06
│   ├── _SUCCESS
│   ├── part-00000-a53a235e-d9b2-4b42-bca2-6948a1fddebc-c000.snappy.parquet
│   ├── part-00001-a53a235e-d9b2-4b42-bca2-6948a1fddebc-c000.snappy.parquet
│   ├── part-00002-a53a235e-d9b2-4b42-bca2-6948a1fddebc-c000.snappy.parquet
│   ├── part-00003-a53a235e-d9b2-4b42-bca2-6948a1fddebc-c000.snappy.parquet
│   ├── part-00004-a53a235e-d9b2-4b42-bca2-6948a1fddebc-c000.snappy.parquet
│   └── part-00005-a53a235e-d9b2-4b42-bca2-6948a1fddebc-c000.snappy.parquet
├── 07
│   ├── _SUCCESS
│   ├── part-00000-f9c46ae7-9912-4093-8f11-f5c558a92ada-c000.snappy.parquet
│   ├── part-00001-f9c46ae7-9912-4093-8f11-f5c558a92ada-c000.snappy.parquet
│   ├── part-00002-f9c46ae7-9912-4093-8f11-f5c558a92ada-c000.snappy.parquet
│   ├── part-00003-f9c46ae7-9912-4093-8f11-f5c558a92ada-c000.snappy.parquet
│   ├── part-00004-f9c46ae7-9912-4093-8f11-f5c558a92ada-c000.snappy.parquet
│   └── part-00005-f9c46ae7-9912-4093-8f11-f5c558a92ada-c000.snappy.parquet
├── 08
│   ├── _SUCCESS
│   ├── part-00000-d43857bc-c7ea-4ef3-8d0b-3ece954362ca-c000.snappy.parquet
│   ├── part-00001-d43857bc-c7ea-4ef3-8d0b-3ece954362ca-c000.snappy.parquet
│   ├── part-00002-d43857bc-c7ea-4ef3-8d0b-3ece954362ca-c000.snappy.parquet
│   ├── part-00003-d43857bc-c7ea-4ef3-8d0b-3ece954362ca-c000.snappy.parquet
│   ├── part-00004-d43857bc-c7ea-4ef3-8d0b-3ece954362ca-c000.snappy.parquet
│   └── part-00005-d43857bc-c7ea-4ef3-8d0b-3ece954362ca-c000.snappy.parquet
├── 09
│   ├── _SUCCESS
│   ├── part-00000-d6b04d56-cb5e-4f18-83a7-8e2a27d16ca4-c000.snappy.parquet
│   ├── part-00001-d6b04d56-cb5e-4f18-83a7-8e2a27d16ca4-c000.snappy.parquet
│   ├── part-00002-d6b04d56-cb5e-4f18-83a7-8e2a27d16ca4-c000.snappy.parquet
│   ├── part-00003-d6b04d56-cb5e-4f18-83a7-8e2a27d16ca4-c000.snappy.parquet
│   ├── part-00004-d6b04d56-cb5e-4f18-83a7-8e2a27d16ca4-c000.snappy.parquet
│   └── part-00005-d6b04d56-cb5e-4f18-83a7-8e2a27d16ca4-c000.snappy.parquet
├── 10
│   ├── _SUCCESS
│   ├── part-00000-859518b8-da05-4eba-9d30-e2c9ab7754ae-c000.snappy.parquet
│   ├── part-00001-859518b8-da05-4eba-9d30-e2c9ab7754ae-c000.snappy.parquet
│   ├── part-00002-859518b8-da05-4eba-9d30-e2c9ab7754ae-c000.snappy.parquet
│   ├── part-00003-859518b8-da05-4eba-9d30-e2c9ab7754ae-c000.snappy.parquet
│   ├── part-00004-859518b8-da05-4eba-9d30-e2c9ab7754ae-c000.snappy.parquet
│   └── part-00005-859518b8-da05-4eba-9d30-e2c9ab7754ae-c000.snappy.parquet
├── 11
│   ├── _SUCCESS
│   ├── part-00000-6147fa73-a73e-4541-8534-ce415e61e203-c000.snappy.parquet
│   ├── part-00001-6147fa73-a73e-4541-8534-ce415e61e203-c000.snappy.parquet
│   ├── part-00002-6147fa73-a73e-4541-8534-ce415e61e203-c000.snappy.parquet
│   ├── part-00003-6147fa73-a73e-4541-8534-ce415e61e203-c000.snappy.parquet
│   ├── part-00004-6147fa73-a73e-4541-8534-ce415e61e203-c000.snappy.parquet
│   └── part-00005-6147fa73-a73e-4541-8534-ce415e61e203-c000.snappy.parquet
└── 12
    ├── _SUCCESS
    ├── part-00000-41140cd6-c3d0-45bc-8160-41f8c64c006f-c000.snappy.parquet
    ├── part-00001-41140cd6-c3d0-45bc-8160-41f8c64c006f-c000.snappy.parquet
    ├── part-00002-41140cd6-c3d0-45bc-8160-41f8c64c006f-c000.snappy.parquet
    ├── part-00003-41140cd6-c3d0-45bc-8160-41f8c64c006f-c000.snappy.parquet
    ├── part-00004-41140cd6-c3d0-45bc-8160-41f8c64c006f-c000.snappy.parquet
    └── part-00005-41140cd6-c3d0-45bc-8160-41f8c64c006f-c000.snappy.parquet
```

> NOTE: The repartition value should match the number of cores that you have to optimize the use of your hardware.

## SQL with Spark

For full details on how to use SQL with spark, take a look at `04_spark_sql.ipynb`.

```python
df_green = spark.read.parquet('data/pq/green/*/*')
df_yellow = spark.read.parquet('data/pq/yellow/*/*')
```

Rename common fields with different names so the same column names:

```python
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
```

Get a list of common column names while still maintaining the original order:

```python
yellow_columns = set(df_yellow.columns)
common_colums = [col for col in df_green.columns if col in yellow_columns]
```

Now select those common columns in spark:

```python
df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))
df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
df_trips_data.groupBy('service_type').count().show()
```

+------------+--------+
|service_type|   count|
+------------+--------+
|       green| 2304517|
|      yellow|39649199|
+------------+--------+

Now lets get the same results with SQL:

```python
df_trips_data.registerTempTable('trips_data')
spark.sql("""
SELECT
    service_type,
    count(1) as count
FROM
    trips_data
GROUP BY 
    service_type
""").show()
```

+------------+--------+
|service_type|   count|
+------------+--------+
|       green| 2304517|
|      yellow|39649199|
+------------+--------+

Another example showing how to export the results from a query:

```python
df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    revenue_zone, revenue_month, service_type
""")
df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')
```

> NOTE: `coalesce()` is used to combine the results into a single file.

```bash
ls -lh data/report/revenue
total 520K
-rw-r--r-- 1 clamytoe clamytoe    0 Mar  2 17:33 _SUCCESS
-rw-r--r-- 1 clamytoe clamytoe 518K Mar  2 17:33 part-00000-d1956e1a-24ef-4480-8244-8243f369a3a8-c000.snappy.parquet
```

## Spark Cluster
