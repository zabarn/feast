import csv, random, uuid, os

import pandas as pd
from datetime import datetime, timezone

data = []
ts = datetime.now(timezone.utc)
for i in range(1000):
    data.append({"rid": i+1, "name": uuid.uuid4(), "vec": [random.uniform(-50.0, 50.0) for _ in range(32)], "timestamp": ts})

with open(f"{os.getcwd()}/data.csv", "w") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["rid", "name", "vec", "timestamp"])
    writer.writeheader()
    for row in data:
        writer.writerow(row)

df = pd.read_csv(f"{os.getcwd()}/data.csv", parse_dates=['timestamp'])
df.to_parquet(f"{os.getcwd()}/data.parquet")
