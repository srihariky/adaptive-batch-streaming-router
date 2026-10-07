import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr
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
    .appName("Adaptive-RuleRouter") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting System 4: Rule-Based Adaptive Router Pipeline...")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", os.environ['TOPIC']) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Add timestamps and router decision log
processed_df = parsed_df \
    .withColumn("router_decision_time", current_timestamp().cast("double")) \
    .withColumn("router_overhead_ms", (col("router_decision_time") - col("event_creation_time")) * 1000)

# Dynamic Split based on urgency rules
stream_branch = processed_df.filter(col("priority") == 1)
batch_branch = processed_df.filter(col("priority") == 0)

# Fast Stream Sink (Urgent Records)
q_stream = stream_branch.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", os.environ["RUN_DIR"] + "/router_stream_out") \
    .option("checkpointLocation", os.environ["RUN_DIR"] + "/checkpoint_router_stream") \
    .trigger(processingTime="2 seconds") \
    .start()

# Thorough Batch Sink (Routine Records)
q_batch = batch_branch.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", os.environ["RUN_DIR"] + "/router_batch_out") \
    .option("checkpointLocation", os.environ["RUN_DIR"] + "/checkpoint_router_batch") \
    .trigger(processingTime="30 seconds") \
    .start()

spark.streams.awaitAnyTermination()
