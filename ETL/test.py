from setup import *

uri = "mongodb://localhost:27017/products_tiki.collection_products_serp"
df = spark.read.format("mongo").option("uri", uri).load()
# df = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")
# df = df.withColumn("completion_time", to_date(df["completion_time"], "yyyy-MM-dd HH:mm:ss.SSSSSS"))
max_date_df_datalake = datetime.datetime.fromtimestamp(0)
df = df.filter(df["completion_time"] > max_date_df_datalake)

selected_col = df.select("completion_time")
output_df = selected_col.\
    withColumn("completion_year", date_format(col("completion_time"), "Y")).\
    withColumn("completion_month", date_format(col("completion_time"), "M")).\
    withColumn("completion_day", date_format(col("completion_time"), "d")).\
    withColumn("completion_hour", date_format(col("completion_time"), "H")).\
    withColumn("completion_minute", date_format(col("completion_time"), "m")).\
    withColumn("completion_second", date_format(col("completion_time"), "s"))
    
print(output_df.show(output_df.count(), truncate=False))

print(output_df.count())

