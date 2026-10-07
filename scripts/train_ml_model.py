import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier
import os

print("Generating dataset and training ML Decision Tree...")

np.random.seed(42)
n_samples = 50000
trip_distance = np.random.exponential(scale=3.0, size=n_samples) + 0.1
fare_amount = trip_distance * 3.5 + np.random.normal(loc=2.5, scale=1.5, size=n_samples)
passenger_count = np.random.choice([1, 2, 3, 4, 5, 6], size=n_samples, p=[0.7, 0.15, 0.05, 0.03, 0.04, 0.03])
optimal_route = np.where((fare_amount > 35.0) | (trip_distance > 10.0) | ((fare_amount > 20.0) & (passenger_count > 3)), 1, 0)

df = pd.DataFrame({'trip_distance': trip_distance, 'fare_amount': fare_amount, 'passenger_count': passenger_count})

# Train ML model
clf = DecisionTreeClassifier(max_depth=4, random_state=42)
clf.fit(df, optimal_route)

# Convert ML tree logic to a PySpark SQL expression
from sklearn.tree import _tree
def get_sql_expr(tree, feature_names):
    tree_ = tree.tree_
    feature_name = [feature_names[i] if i != _tree.TREE_UNDEFINED else "undefined" for i in tree_.feature]
    
    def recurse(node):
        if tree_.feature[node] != _tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            left = recurse(tree_.children_left[node])
            right = recurse(tree_.children_right[node])
            return f"CASE WHEN {name} <= {threshold:.2f} THEN {left} ELSE {right} END"
        else:
            return str(int(np.argmax(tree_.value[node][0])))
    return recurse(0)

sql_expr = get_sql_expr(clf, df.columns)

os.makedirs("data", exist_ok=True)
with open("data/ml_sql_expr.txt", "w") as f:
    f.write(sql_expr)

print("ML model trained and converted to native SQL expression successfully.")
