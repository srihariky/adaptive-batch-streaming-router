import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from work import start_spark, read_parsed, run_query, with_route
spark = start_spark("StreamOnly-Kappa")
run_query(read_parsed(spark), "stream_only", "heavy", "2 seconds")
spark.streams.awaitAnyTermination()
