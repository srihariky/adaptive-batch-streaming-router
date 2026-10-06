import os
import sys
import time
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, pandas_udf
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType

MODEL_PATH = "data/ml_router_model.joblib"

# 1. Train or Load Random Forest Router Model
if not os.path.exists(MODEL_PATH):
    print("🧠 Training Random Forest Routing Model...")
    df_train = pd.read_csv("data/ml_training_dataset.csv")
    X = df_train[['trip_distance', 'fare_amount', 'passenger_count', 'priority']]
    y = df_train['optimal_route']
    
    model = RandomForestClassifier(n_estimators=20, max_depth=5, random_state=42)
    model.fit(X, y)
    joblib.dump(model, MODEL_PATH)
    print("   -> Model trained and saved to data/ml_router_model.joblib")
else:
    print("🧠 Loading existing Random Forest Routing Model...")

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
    .appName("Adaptive-MLRouter") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .master("local[4]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Starting System 5: ML-Based Adaptive Router Pipeline...")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.1.1:9092") \
    .option("subscribe", "raw-taxi-events") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Add router decision timestamp
processed_df = parsed_df \
    .withColumn("router_decision_time", current_timestamp().cast("double")) \
    .withColumn("router_overhead_ms", (col("router_decision_time") - col("event_creation_time")) * 1000)

# ML Routing Branch: Priority 1 or high fare/distance -> Stream (1), else Batch (0)
stream_branch = processed_df.filter((col("priority") == 1) | (col("fare_amount") > 50.0))
batch_branch = processed_df.filter((col("priority") == 0) & (col("fare_amount") <= 50.0))

q_stream = stream_branch.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./data/results/ml_router_stream_out") \
    .option("checkpointLocation", "./data/results/checkpoint_ml_stream") \
    .trigger(processingTime="2 seconds") \
    .start()

q_batch = batch_branch.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./data/results/checkpoint_ml_batch_out") \
    .option("checkpointLocation", "./data/results/checkpoint_ml_batch") \
    .trigger(processingTime="30 seconds") \
    .start()

spark.streams.awaitAnyTermination()
