# transform from HDFS datalake to HDFS data warehouse
from setup import *
from pyarrow import fs

# load data to spark df
# collection_products_serp_path = "hdfs://localhost:19000/datalake/collection_products_serp"
# collection_products_detail_path = "hdfs://localhost:19000//datalake/collection_products_detail"    
# collection_products_serp_df = spark.read.parquet(collection_products_serp_path)
# collection_products_detail_df = spark.read.parquet(collection_products_detail_path)

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

dl_collection_products_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")
dw_Dim_Category = spark.read.format("parquet").load("hdfs://localhost:19000/datawarehouse/Dim_Category")

# selected_col = dl_collection_products_detail.select("category_id","category_name")
print(dw_Dim_Category.rdd.isEmpty())

#thêm schema cho bảng dwh