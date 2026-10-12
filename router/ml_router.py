import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from work import start_spark, read_parsed, run_query, with_route
SQL_PATH = "data/ml_sql_expr.txt"
if not os.path.exists(SQL_PATH):
    raise FileNotFoundError("Train the ML router first.")
ML_SQL = open(SQL_PATH).read().strip()
spark = start_spark("Adaptive-MLRouter")
routed = with_route(read_parsed(spark), ML_SQL)
run_query(routed.filter("route = 1"), "ml_router_stream", "light", "2 seconds")
run_query(routed.filter("route = 0"), "ml_router_batch", "heavy", "30 seconds")
spark.streams.awaitAnyTermination()
