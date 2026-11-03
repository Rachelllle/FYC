import os
os.environ.pop("SPARK_HOME", None)
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

builder = (SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
           .config("spark.sql.catalog.local.type", "hadoop")
           .config("spark.sql.catalog.local.warehouse", "data/iceberg_warehouse"))
spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"]).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Lignes version 0 :", spark.read.format("delta").option("versionAsOf", 0).load("data/silver/taxis_delta").count())
print("Lignes version actuelle :", spark.read.format("delta").load("data/silver/taxis_delta").count())

DeltaTable.forPath(spark, "data/silver/taxis_delta").restoreToVersion(1)
print("Historique après RESTORE :")
DeltaTable.forPath(spark, "data/silver/taxis_delta").history().select("version", "operation").orderBy("version").show()

nouvelles = spark.read.format("delta").load("data/silver/taxis_delta").limit(5).withColumn("tip_ratio", F.col("tip_amount") / F.col("total_amount"))
nouvelles.write.format("delta").mode("append").option("mergeSchema", "true").save("data/silver/taxis_delta")
print("Schéma avec tip_ratio :")
spark.read.format("delta").load("data/silver/taxis_delta").printSchema()

spark.sql("DROP TABLE IF EXISTS local.db.taxis PURGE")
spark.read.format("delta").option("versionAsOf", 0).load("data/silver/taxis_delta").writeTo("local.db.taxis").partitionedBy(F.days("tpep_pickup_datetime")).create()
print("Lignes Iceberg :", spark.table("local.db.taxis").count())

spark.sql("UPDATE local.db.taxis SET passenger_count = 1 WHERE passenger_count = 0")
spark.sql("DELETE FROM local.db.taxis WHERE fare_amount > 500")
print("Lignes après UPDATE et DELETE :", spark.table("local.db.taxis").count())

print("Snapshots :")
snapshots = spark.sql("SELECT snapshot_id, committed_at, operation FROM local.db.taxis.snapshots ORDER BY committed_at")
snapshots.show(truncate=False)
print("Lignes au premier snapshot :")
spark.sql(f"SELECT COUNT(*) FROM local.db.taxis VERSION AS OF {snapshots.first()['snapshot_id']}").show()

spark.sql("ALTER TABLE local.db.taxis ADD COLUMN commentaire STRING")
spark.sql("ALTER TABLE local.db.taxis REPLACE PARTITION FIELD days(tpep_pickup_datetime) WITH months(tpep_pickup_datetime)")
print("Table après ALTER :")
spark.sql("DESCRIBE TABLE local.db.taxis").show(30, truncate=False)
