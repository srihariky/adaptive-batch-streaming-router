"""Shared pipeline code: identical fast/thorough work and writers for every system."""
import os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType

TOPIC = os.environ.get("TOPIC", "")
RUN_DIR = os.environ.get("RUN_DIR", "data/results/tmp")
BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")
CORES = os.environ.get("SPARK_CORES", "4")
SHUFFLE_PARTS = os.environ.get("SHUFFLE_PARTS", "8")
ZONES_PATH = os.environ.get("ZONES_PATH", "data/taxi_zone_lookup.csv")

SCHEMA = StructType([
    StructField("record_id", LongType()),
    StructField("vendor_id", DoubleType()),
    StructField("pickup_datetime", StringType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("pickup_location_id", DoubleType()),
    StructField("dropoff_location_id", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("priority", IntegerType()),
    StructField("arrival_rate", DoubleType()),
    StructField("event_creation_time", DoubleType()),
])

_zones = {}

def start_spark(name):
    spark = (SparkSession.builder.appName(name)
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
             .config("spark.sql.shuffle.partitions", SHUFFLE_PARTS)
             .config("spark.ui.enabled", "false")
             .master(f"local[{CORES}]").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_parsed(spark):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest").load())
    return raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d")).select("d.*")

def with_route(df, route_sql):
    return (df.withColumn("fare_per_mile", F.col("fare_amount") / F.greatest(F.col("trip_distance"), F.lit(0.1)))
              .withColumn("route", F.expr(route_sql).cast("int")))

def _with_fpm(d):
    return d.withColumn("fare_per_mile", F.col("fare_amount") / F.greatest(F.col("trip_distance"), F.lit(0.1)))

def light(d):
    return _with_fpm(d).withColumn("thorough", F.lit(0))

def heavy(d, zones):
    d = _with_fpm(d)
    ok = ((F.col("fare_amount") > 0) & (F.col("fare_amount") < 500) & (F.col("trip_distance") > 0)
          & (F.col("trip_distance") < 100) & F.col("passenger_count").between(1, 6)
          & F.col("pickup_datetime").isNotNull() & (~F.col("pickup_datetime").isin("None", "NaT", "nan")))
    d = d.withColumn("valid", ok.cast("int"))
    pu = zones.select(F.col("LocationID").alias("pu_id"), F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
    do = zones.select(F.col("LocationID").alias("do_id"), F.col("Borough").alias("do_borough"))
    d = (d.join(F.broadcast(pu), F.col("pickup_location_id").cast("int") == F.col("pu_id"), "left").drop("pu_id")
          .join(F.broadcast(do), F.col("dropoff_location_id").cast("int") == F.col("do_id"), "left").drop("do_id"))
    st = (d.filter("valid = 1").groupBy("pickup_location_id")
            .agg(F.expr("percentile_approx(fare_per_mile, array(0.25, 0.75))").alias("q"), F.count(F.lit(1)).alias("zn"))
            .select("pickup_location_id",
                    F.when(F.col("zn") >= 5, F.col("q")[1] + 3.0 * (F.col("q")[1] - F.col("q")[0])).alias("fence")))
    d = (d.join(st, "pickup_location_id", "left")
          .withColumn("is_outlier", F.when((F.col("valid") == 1) & F.col("fence").isNotNull()
                                           & (F.col("fare_per_mile") > F.col("fence")), 1).otherwise(0))
          .drop("fence").withColumn("thorough", F.lit(1)))
    return d

def zone_aggregates(d):
    return (d.filter("valid = 1").groupBy("pickup_location_id")
              .agg(F.count(F.lit(1)).alias("trips"), F.sum("fare_amount").alias("fare_sum"),
                   F.avg("trip_distance").alias("avg_dist"), F.avg("fare_per_mile").alias("avg_fpm")))

def get_zones(spark):
    if "z" not in _zones:
        _zones["z"] = spark.read.csv(ZONES_PATH, header=True, inferSchema=True).cache()
    return _zones["z"]

def make_writer(name, mode):
    out, agg_out = f"{RUN_DIR}/{name}_out", f"{RUN_DIR}/{name}_agg"
    def fn(batch_df, batch_id):
        t_start = time.time()
        b = batch_df.persist()
        if b.isEmpty():
            b.unpersist(); return
        if mode == "heavy":
            res = heavy(b, get_zones(b.sparkSession)).persist()
            res.count()
            agg = zone_aggregates(res).persist()
            agg.count()
        else:
            res = light(b).persist()
            res.count()
            agg = None
        t = time.time()
        res.withColumn("spark_process_time", F.lit(t)).withColumn("batch_start_time", F.lit(t_start)) \
           .write.mode("append").parquet(out)
        if agg is not None:
            agg.withColumn("spark_process_time", F.lit(t)).write.mode("append").parquet(agg_out)
            agg.unpersist()
        res.unpersist(); b.unpersist()
    return fn

def run_query(df, name, mode, trigger):
    return (df.writeStream.foreachBatch(make_writer(name, mode))
              .option("checkpointLocation", f"{RUN_DIR}/checkpoint_{name}")
              .trigger(processingTime=trigger).start())
