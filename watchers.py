import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def watchers_update(path):
    spark = SparkSession.builder.master("local").appName("watchers_update").getOrCreate()
    for file in os.listdir(path):
        if file.endswith(".json"):
            batch_path = os.path.join(path,file)
            batch = spark.read.json(batch_path).filter(col("type") == "WatchEvent")
            batch.select(col("actor.id").alias("user_id"), col("repo.id").alias("repo_id"),
                         col("created_at")).write.option("header", "true").csv(path)