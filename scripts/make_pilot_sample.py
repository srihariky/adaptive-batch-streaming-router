import sys, pandas as pd
raw = pd.read_parquet(sys.argv[1])
final = pd.read_parquet("data/sampled_taxi_data.parquet")
key = ["tpep_pickup_datetime", "PULocationID", "DOLocationID", "fare_amount", "trip_distance"]
m = raw.merge(final[key].drop_duplicates(), on=key, how="left", indicator=True)
pool = m[m["_merge"] == "left_only"].drop(columns="_merge")
pilot = pool.sample(n=len(final), random_state=7)
pilot = pilot[[c for c in final.columns if c in pilot.columns]]
pilot.to_parquet("data/pilot_sample.parquet")
print(f"pilot sample: {len(pilot):,} rows, disjoint from final sample (pool {len(pool):,})")
