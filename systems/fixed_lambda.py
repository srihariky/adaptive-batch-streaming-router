import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from work import start_spark, read_parsed, run_query, with_route
spark = start_spark("FixedLambda")
df = read_parsed(spark)
run_query(df, "lambda_stream", "light", "2 seconds")
run_query(df, "lambda_batch", "heavy", "30 seconds")
spark.streams.awaitAnyTermination()
