import numpy as np

RATE_HIGH = 2500.0                       # rec/s: between steady (1000) and burst (5000)
STRICT_FARE, STRICT_DIST = 50.0, 15.0    # used under high load
LOOSE_FARE, LOOSE_DIST = 30.0, 8.0       # used when fast path has spare capacity

RULE_SQL = (
    f"CASE WHEN arrival_rate >= {RATE_HIGH} "
    f"THEN (CASE WHEN fare_amount > {STRICT_FARE} OR trip_distance > {STRICT_DIST} THEN 1 ELSE 0 END) "
    f"ELSE (CASE WHEN fare_amount > {LOOSE_FARE} OR trip_distance > {LOOSE_DIST} THEN 1 ELSE 0 END) END"
)

def rule_route_pandas(df):
    high = df["arrival_rate"] >= RATE_HIGH
    strict = (df["fare_amount"] > STRICT_FARE) | (df["trip_distance"] > STRICT_DIST)
    loose = (df["fare_amount"] > LOOSE_FARE) | (df["trip_distance"] > LOOSE_DIST)
    return np.where(high, strict, loose).astype(int)
