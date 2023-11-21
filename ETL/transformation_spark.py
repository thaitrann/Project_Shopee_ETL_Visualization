# transform from HDFS datalake to HDFS data warehouse
from setup import *

# load data to spark df
# collection_products_serp_path = "hdfs://localhost:19000/datalake/collection_products_serp"
# collection_products_detail_path = "hdfs://localhost:19000//datalake/collection_products_detail"    
# collection_products_serp_df = spark.read.parquet(collection_products_serp_path)
# collection_products_detail_df = spark.read.parquet(collection_products_detail_path)

fs = FileSystem.get(URI("hdfs://localhost:19000"), Configuration())
exists = fs.exists(Path('/datalake/collection_products_serp'))

#lastest in datalake
location_datalake = "hdfs://localhost:19000/datalake/collection_products_serp"
if exists:
    df_datalake = spark.read.parquet(location_datalake)
    max_date_df_datalake = df_datalake.select(F.max(F.col("completion_time"))).rdd.first()[0]
else:
    max_date_df_datalake = datetime.datetime.fromtimestamp(0)

print(df_datalake.show())

#lấy year, month, day trong datalake làm partition cho bảng dim.
#tạo bảng dim bằng tổng hợp bảng trong datalake, lấy dòng mới nhất lưu vào.