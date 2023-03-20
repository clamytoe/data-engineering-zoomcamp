import pyspark.sql.types as T

FHV_DATA_PATH = "data/fhv/fhv_tripdata_2019-01.csv"
GREEN_DATA_PATH = "data/green/green_tripdata_2019-01.csv"
BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_BROKERS = "localhost:9090,localhost:9091,localhost:9092"

TOPIC_WINDOWED_VENDOR_ID_COUNT = "vendor_counts_windowed"
TOPIC_WINDOWED_DISPATCHING_BASE_NUM_COUNT = "dispatching_counts_windowed"

PRODUCE_FHV_TOPIC_RIDES_CSV = CONSUME_FHV_TOPIC_RIDES_CSV = "fhv_rides_csv"
PRODUCE_GREEN_TOPIC_RIDES_CSV = CONSUME_GREEN_TOPIC_RIDES_CSV = "green_rides_csv"
RIDES_ALL_TOPIC = "rides_all"

CONFIG = {
    "bootstrap_servers": [BOOTSTRAP_SERVERS],
    "key_serializer": lambda x: x.encode("utf-8"),
    "value_serializer": lambda x: x.encode("utf-8"),
}

GREEN_SCHEMA = T.StructType(
    [
        T.StructField("VendorID", T.IntegerType()),
        T.StructField("lpep_pickup_datetime", T.TimestampType()),
        T.StructField("lpep_dropoff_datetime", T.TimestampType()),
        T.StructField("store_and_fwd_flag", T.StringType()),
        T.StructField("RatecodeID", T.IntegerType()),
        T.StructField("PULocationID", T.IntegerType()),
        T.StructField("DOLocationID", T.IntegerType()),
        T.StructField("passenger_count", T.IntegerType()),
        T.StructField("trip_distance", T.FloatType()),
        T.StructField("fare_amount", T.FloatType()),
        T.StructField("extra", T.FloatType()),
        T.StructField("mta_tax", T.FloatType()),
        T.StructField("tip_amount", T.FloatType()),
        T.StructField("tolls_amount", T.FloatType()),
        T.StructField("ehail_fee", T.FloatType()),
        T.StructField("improvement_surcharge", T.FloatType()),
        T.StructField("total_amount", T.FloatType()),
        T.StructField("payment_type", T.IntegerType()),
        T.StructField("trip_type", T.IntegerType()),
        T.StructField("congestion_surcharge", T.FloatType()),
    ]
)

FHV_SCHEMA = T.StructType(
    [
        T.StructField("dispatching_base_num", T.StringType()),
        T.StructField("pickup_datetime", T.TimestampType()),
        T.StructField("dropOff_datetime", T.TimestampType()),
        T.StructField("PUlocationID", T.FloatType()),
        T.StructField("DOlocationID", T.FloatType()),
        T.StructField("SR_Flag", T.FloatType()),
        T.StructField("Affiliated_base_number", T.StringType()),
    ]
)
