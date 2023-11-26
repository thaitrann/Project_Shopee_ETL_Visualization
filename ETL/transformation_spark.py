# transform from HDFS datalake to HDFS data warehouse
from setup import *
from pyarrow import fs

# load data to spark df
# collection_products_serp_path = "hdfs://localhost:19000/datalake/collection_products_serp"
# collection_products_detail_path = "hdfs://localhost:19000//datalake/collection_products_detail"    
# collection_products_serp_df = spark.read.parquet(collection_products_serp_path)
# collection_products_detail_df = spark.read.parquet(collection_products_detail_path)

fs = FileSystem.get(URI("hdfs://localhost:19000"), Configuration())
exists = fs.exists(Path('/datalake/collection_products_serp'))

dwh_tables = ["Dim_Category", "Dim_Product", "Dim_Inventory", "Dim_Seller", "Dim_Star", "Dim_Brand", "Dim_Shipping", \
    "Dim_Gift", "Dim_Url", "Dim_Time", "Dim_ConfigurableProduct", "Fact_Sales", "Fact_Product"]

def create_table_hadoop(dwh_tables):
    for table in dwh_tables:
        path = "/datawarehouse/{}".format(table)
        if not fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)):
            fs.mkdirs(Path(path))
            print("Table created: {}!".format(table))
        else:
            print("Table already exists: {}!".format(table))

#create_table_hadoop(dwh_tables)

fs = FileSystem.get(URI("hdfs://localhost:19000"), Configuration())
exists = fs.exists(Path('/datawarehouse/Dim_Category'))
location_datawarehouse = 'hdfs://localhost:19000/datawarehouse/Dim_Category'
if exists:
    df_datawarehouse = spark.read.parquet(location_datawarehouse)
    max_date_df_datawarehouse = df_datawarehouse.select(F.max(F.col("completion_time"))).rdd.first()[0]
else:
    max_date_df_datawarehouse = datetime.datetime.fromtimestamp(0)

print(max_date_df_datawarehouse)


#lấy year, month, day trong datalake làm partition cho bảng dim.
#tạo bảng dim bằng tổng hợp bảng trong datalake, lấy dòng mới nhất lưu vào.