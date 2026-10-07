import os
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType

SQL_PATH = "data/ml_sql_expr.txt"
if not os.path.exists(SQL_PATH):
    raise FileNotFoundError("Model SQL missing! Run scripts/train_ml_model.py first.")

with open(SQL_PATH, "r") as f:
    ml_sql_expression = f.read().strip()

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

raw_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", os.environ['TOPIC']).option("startingOffsets", "earliest").load()
parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Apply ML logic via native Spark SQL (bypasses Python 3.14 pickling bug)
routed_df = parsed_df.withColumn("predicted_route", expr(ml_sql_expression).cast("int")) \
    .withColumn("router_decision_time", current_timestamp().cast("double"))

stream_branch = routed_df.filter(col("predicted_route") == 1)
batch_branch = routed_df.filter(col("predicted_route") == 0)

q_stream = stream_branch.writeStream.outputMode("append").format("parquet").option("path", os.environ["RUN_DIR"] + "/ml_router_stream_out").option("checkpointLocation", os.environ["RUN_DIR"] + "/checkpoint_ml_stream").trigger(processingTime="2 seconds").start()
q_batch = batch_branch.writeStream.outputMode("append").format("parquet").option("path", os.environ["RUN_DIR"] + "/ml_router_batch_out").option("checkpointLocation", os.environ["RUN_DIR"] + "/checkpoint_ml_batch").trigger(processingTime="30 seconds").start()

spark.streams.awaitAnyTermination()
