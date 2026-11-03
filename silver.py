import os, shutil
os.environ.pop("SPARK_HOME", None)
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

builder = (SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

bronze = spark.read.parquet("data/bronze/taxis_parquet")
print("Lignes Bronze :", bronze.count())

silver = (bronze.dropna(subset=["passenger_count", "trip_distance", "fare_amount"])
          .filter("trip_distance > 0 AND fare_amount > 0")
          .withColumn("trip_id", F.monotonically_increasing_id()))
shutil.rmtree("data/silver/taxis_delta", ignore_errors=True)
silver.write.format("delta").mode("overwrite").save("data/silver/taxis_delta")
print("Lignes Silver :", silver.count())
print("_delta_log :", os.listdir("data/silver/taxis_delta/_delta_log"))

table = DeltaTable.forPath(spark, "data/silver/taxis_delta")
table.update("passenger_count = 0", {"passenger_count": "1"})
print("0 passager après UPDATE :", table.toDF().filter("passenger_count = 0").count())
table.delete("fare_amount > 500")
print("Tarifs > 500 après DELETE :", table.toDF().filter("fare_amount > 500").count())

print("Avant MERGE :")
table.toDF().orderBy("trip_id").limit(3).select("trip_id", "fare_amount").show()
modifs = table.toDF().orderBy("trip_id").limit(3).withColumn("fare_amount", F.round(F.col("fare_amount") * 1.1, 2))
table.alias("t").merge(modifs.alias("m"), "t.trip_id = m.trip_id").whenMatchedUpdateAll().execute()
print("Après MERGE :")
table.toDF().orderBy("trip_id").limit(3).select("trip_id", "fare_amount").show()

try:
    table.toDF().limit(1).withColumn("colonne_en_trop", F.lit("test")).write.format("delta").mode("append").save("data/silver/taxis_delta")
except Exception as e:
    print("Écriture refusée :", str(e).splitlines()[0])

print("Historique :")
table.history().select("version", "operation").orderBy("version").show()
