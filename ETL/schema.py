from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from setup import *

dwh_tables = ["Dim_Category", "Dim_Product", "Dim_Inventory", "Dim_Seller", "Dim_Star", "Dim_Brand", "Dim_Shipping", \
    "Dim_Gift", "Dim_Url", "Dim_Time", "Dim_ConfigurableProduct", "Fact_Sales", "Fact_Product"]\
        
def create_tables(dwh_tables):
    for table in dwh_tables:
        path = "/datawarehouse/{}".format(table)
        if not fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)):
            fs.mkdirs(Path(path))
            print("Table created: {}!".format(table))
        else:
            print("Table already exists: {}!".format(table))
            
# create_tables(dwh_tables)

dim_category_schema = StructType([
    StructField("category_id", IntegerType(), nullable = False),
    StructField("category_name", StringType(), nullable = False)
])
location_table_dwh = 'hdfs://localhost:19000/datawarehouse/Dim_Category'
dim_category_df = spark.read.schema(dim_category_schema).parquet(location_table_dwh)



