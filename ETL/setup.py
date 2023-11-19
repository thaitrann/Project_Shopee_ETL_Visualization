import os
import findspark
findspark.init("C:\spark-3.2.4-bin-hadoop3.2")
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format
from pyspark.sql.functions import col
import time
start_time = time.time()
import pyspark.sql.functions as F
import datetime

#config
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
spark = SparkSession.builder.appName('ETL Application').\
    config("spark.driver.bindAddress","localhost").\
    config("spark.ui.port","4040").\
    config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").\
    getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sparkContext.setLogLevel("off")
sc = spark.sparkContext

URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path 
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration