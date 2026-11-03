import pandas as pd

# Échantillon aléatoire de 200 000 courses, enregistré en CSV
df = pd.read_parquet("data/raw/yellow_tripdata_2024-01.parquet")
df.sample(n=200_000, random_state=42).to_csv("data/raw/yellow_tripdata_2024-01.csv", index=False)
