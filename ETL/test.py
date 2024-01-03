from setup import *

df = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")

time = df.select("completion_time").distinct()

print(time.show())