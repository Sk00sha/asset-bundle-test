from pyspark.sql import functions as F

TABLE = "default.random_numbers"

df = (
    spark.range(0, 100)
         .withColumn("random_value", F.rand())
         .withColumn("ingest_ts", F.current_timestamp())
)

# Overwrite table each run
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(TABLE)

print(f"Written {df.count()} rows to {TABLE}")
