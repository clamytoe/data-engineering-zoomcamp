from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, from_json
from pyspark.sql.types import FloatType, StructField, StructType
from settings import (
    FHV_SCHEMA,
    GREEN_SCHEMA,
    KAFKA_BROKERS,
    PRODUCE_FHV_TOPIC_RIDES_CSV,
    PRODUCE_GREEN_TOPIC_RIDES_CSV,
    RIDES_ALL_TOPIC,
)

# Define the schema for the pickup location ID
pickup_location_schema = StructType([StructField("pickup_location_id", FloatType())])

# Create a SparkSession
spark = SparkSession.builder.appName("popular_pickup_locations").getOrCreate()

# Read from the Green and FHV Kafka topics and write them to the "RIDES_ALL_TOPIC" topic
df_green = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", PRODUCE_GREEN_TOPIC_RIDES_CSV)
    .option("startingOffsets", "earliest")
    .load()
)

df_green_consume = (
    df_green.select(
        from_json(df_green.value.cast("string"), GREEN_SCHEMA).alias("data")
    )
    .select("data.PULocationID")
    .withColumnRenamed("PULocationID", "pickup_location_id")
)

df_fhv = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", PRODUCE_FHV_TOPIC_RIDES_CSV)
    .option("startingOffsets", "earliest")
    .load()
)

df_fhv_consume = (
    df_fhv.select(from_json(df_fhv.value.cast("string"), FHV_SCHEMA).alias("data"))
    .select("data.PUlocationID")
    .withColumnRenamed("PUlocationID", "pickup_location_id")
)

df_all_rides = df_green_consume.union(df_fhv_consume)

# Apply aggregations to find the most popular pickup location
popular_pickup_locations = (
    df_all_rides.groupBy("pickup_location_id")
    .agg(count("*").alias("total_rides"))
    .orderBy(desc("total_rides"))
)

# Write the results to the "popular_pickup_locations" Kafka topic
popular_pickup_locations.selectExpr(
    "CAST(pickup_location_id AS STRING) AS key", "to_json(struct(*)) AS value"
).writeStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKERS).option(
    "topic", RIDES_ALL_TOPIC
).option(
    "checkpointLocation", "/tmp/checkpoints"
).start().awaitTermination()
