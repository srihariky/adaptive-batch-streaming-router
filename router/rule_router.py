import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from work import start_spark, read_parsed, run_query, with_route
from params import RULE_SQL
spark = start_spark("Adaptive-RuleRouter")
routed = with_route(read_parsed(spark), RULE_SQL)
run_query(routed.filter("route = 1"), "router_stream", "light", "2 seconds")
run_query(routed.filter("route = 0"), "router_batch", "heavy", "30 seconds")
spark.streams.awaitAnyTermination()
