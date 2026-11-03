import os
os.environ.pop("SPARK_HOME", None)
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/raw/yellow_tripdata_2024-01.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)
print("Lignes :", df.count(), "| Colonnes :", len(df.columns))

print("Valeurs manquantes :")
df.select([F.count_if(F.col(c).isNull()).alias(c) for c in df.columns]).show(vertical=True)
print("Distances <= 0 :", df.filter("trip_distance <= 0").count())
print("Tarifs <= 0 :", df.filter("fare_amount <= 0").count())
print("0 passager :", df.filter("passenger_count = 0").count())

df.write.mode("overwrite").parquet("data/bronze/taxis_parquet")
print("Lignes relues :", spark.read.parquet("data/bronze/taxis_parquet").count())
print("Contenu Bronze :", os.listdir("data/bronze/taxis_parquet"))
