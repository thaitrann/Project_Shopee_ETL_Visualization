import os
import findspark
findspark.init("C:\spark-3.2.4-bin-hadoop3.2")
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql.functions import col
import time
start_time = time.time()

#config
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
spark = SparkSession.builder.appName('Ingestion - from MONGODB to HDFS').\
    config("spark.driver.bindAddress","localhost").\
    config("spark.ui.port","4040").\
    config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").\
    getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.rpc.maxmessagesize", "10000000")
spark.sparkContext.setLogLevel("off")
sc = spark.sparkContext

URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path 
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

collection = 'collection_products_serp'
uri = "mongodb://localhost:27017/products_tiki.{}".format(collection)
df = spark.read.format("mongo").option("uri", uri).load()

# Print the data
df = df.withColumn("completion_time", to_timestamp(col("completion_time"))).\
    withColumn("completion_year", date_format(col("completion_time"), "Y")).\
    withColumn("completion_month", date_format(col("completion_time"), "M")).\
    withColumn("completion_day", date_format(col("completion_time"), "d"))
    
df.select('completion_time', 'completion_year', 'completion_month', 'completion_day').show()
fs = FileSystem.get(URI("hdfs://localhost:19000"), Configuration())
print(fs.exists(Path('/datalake')))

print(f"Execution time: {time.time() - start_time}")