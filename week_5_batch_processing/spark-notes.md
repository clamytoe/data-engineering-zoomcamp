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
