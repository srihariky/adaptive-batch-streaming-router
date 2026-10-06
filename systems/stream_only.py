import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType

schema = StructType([
    StructField("record_id", LongType()),
    StructField("vendor_id", DoubleType()),
    StructField("pickup_datetime", StringType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("pickup_location_id", DoubleType()),
    StructField("dropoff_location_id", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("priority", IntegerType()),
    StructField("event_creation_time", DoubleType())
])

spark = SparkSession.builder \
    .appName("Baseline-StreamOnly") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting System 1: Stream-Only Baseline Pipeline...")

# Read Kafka Stream
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw-taxi-events") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Add processing timestamps
processed_df = parsed_df \
    .withColumn("spark_process_time", current_timestamp().cast("double")) \
    .withColumn("system_type", col("priority") * 0 + 1) # Tagged stream

# Append to Parquet sink with 2-second streaming trigger
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./data/results/stream_only_out") \
    .option("checkpointLocation", "./data/results/checkpoint_stream_only") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()
