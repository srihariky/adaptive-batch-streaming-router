import os
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
    .appName("Baseline-BatchOnly") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting System 2: Batch-Only Baseline Pipeline...")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", os.environ['TOPIC']) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

processed_df = parsed_df \
    .withColumn("spark_process_time", current_timestamp().cast("double")) \
    .withColumn("system_type", col("priority") * 0 + 2) # Tagged batch

# Batch processing schedule (30-second triggers)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", os.environ["RUN_DIR"] + "/batch_only_out") \
    .option("checkpointLocation", os.environ["RUN_DIR"] + "/checkpoint_batch_only") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
