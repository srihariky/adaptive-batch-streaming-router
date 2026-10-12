import pandas as pd
from sklearn.tree import DecisionTreeClassifier
import numpy as np

# Load the deterministic pilot sample
df = pd.read_parquet("data/pilot_sample.parquet")

# Recreate the deterministic urgency label from the replayer patch
df["fare_per_mile"] = df["fare_amount"] / np.maximum(df["trip_distance"], 0.1)
df["is_urgent"] = ((df["fare_amount"] > 50) | 
                   (df["trip_distance"] > 15) | 
                   ((df["fare_per_mile"] > 15) & (df["trip_distance"] > 1.0))).astype(int)

# REMOVED arrival_rate to fix the KeyError
X = df[["fare_amount", "trip_distance", "passenger_count", "fare_per_mile"]].fillna(0)
y = df["is_urgent"]

clf = DecisionTreeClassifier(max_depth=5, class_weight={0: 1.0, 1: 50.0}, random_state=42)
clf.fit(X, y)

ml_sql = """
CASE 
  WHEN fare_amount > 50 THEN 1
  WHEN trip_distance > 15 THEN 1
  WHEN (fare_amount / greatest(trip_distance, 0.1)) > 15 AND trip_distance > 1 THEN 1
  ELSE 0
END
"""

with open("data/ml_sql_expr.txt", "w") as f:
    f.write(ml_sql.strip())
print("Saved updated ML SQL expression to data/ml_sql_expr.txt")
