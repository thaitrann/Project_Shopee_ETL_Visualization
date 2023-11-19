# transform from HDFS datalake to HDFS data warehouse
from setup import *

# load data to spark df
collection_products_serp_path = "hdfs://localhost:19000/datalake/collection_products_serp"
collection_products_detail_path = "hdfs://localhost:19000//datalake/collection_products_detail"    
collection_products_serp_df = spark.read.parquet(collection_products_serp_path)
collection_products_detail_df = spark.read.parquet(collection_products_detail_path)
