from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, DoubleType
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
def create_schema_location(table_name):
    location_table_dwh = 'hdfs://localhost:19000/datawarehouse/{}'.format(table_name)
    dim_category_df = spark.read.schema(dim_category_schema).parquet(location_table_dwh)
    return location_table_dwh, dim_category_df

table_name = "Dim_Category"
test_location, test_df = create_schema_location(table_name)
print(test_location)
print(test_df.printSchema())
#schema
#Dim_Category
dim_category_schema = StructType([
    StructField("category_id", IntegerType(), nullable = False),
    StructField("category_name", StringType(), nullable = False)
])
location_table_dwh = 'hdfs://localhost:19000/datawarehouse/Dim_Category'
dim_category_df = spark.read.schema(dim_category_schema).parquet(location_table_dwh)

#Dim_Product
dim_product_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False)
])

#Dim_ConfigurableProduct
dim_configurable_product_schema = StructType([
    StructField("configurable_product_id", IntegerType(), nullable=False),
    StructField("configurable_product_name", StringType(), nullable=False),
    StructField("configurable_product_type", StringType(), nullable=False),
    StructField("configurable_product_price", DoubleType(), nullable=False),
    StructField("configurable_product_status", StringType(), nullable=False)
])

#Dim_Inventory
dim_inventory_schema = StructType([
    StructField("inventory_sgg_id", IntegerType(), nullable=False),
    StructField("inventory_status", StringType(), nullable=False),
    StructField("inventory_type", StringType(), nullable=False)
])

#Dim_Seller
dim_seller_schema = StructType([
    StructField("seller_id", IntegerType(), nullable=False),
    StructField("seller_name", StringType(), nullable=False)
])

#Dim_Star
dim_star_schema = StructType([
    StructField("star_sgg_id", IntegerType(), nullable=False),
    StructField("star_count", IntegerType(), nullable=False),
    StructField("star_percent", IntegerType(), nullable=False)
])

#Dim_Brand
dim_brand_schema = StructType([
    StructField("brand_id", IntegerType(), nullable=False),
    StructField("brand_name", StringType(), nullable=False)
])

#Dim_Shipping
dim_shipping_schema = StructType([
    StructField("shipping_sgg_id", IntegerType(), nullable=False),
    StructField("shipping_code", StringType(), nullable=False),
    StructField("shipping_text", StringType(), nullable=False)
])

#Dim_Gift
dim_gift_schema = StructType([
    StructField("gift_sgg_id", IntegerType(), nullable=False),
    StructField("gift_item_title", StringType(), nullable=False)
])

#Dim_Url
dim_url_schema = StructType([
    StructField("url_sgg_id", IntegerType(), nullable=False),
    StructField("url", StringType(), nullable=False)
])

#Dim_Time
dim_time_schema = StructType([
    StructField("time_sgg_id", IntegerType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("day_of_week", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("quarter", IntegerType(), nullable=False),
    StructField("year", IntegerType(), nullable=False)
])

#Fact_Sales
fact_sales_schema = StructType([
    StructField("category_id", IntegerType()),
    StructField("brand_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("gift_sgg_id", IntegerType()),
    StructField("inventory_sgg_id", IntegerType()),
    StructField("url_sgg_id", StringType()),
    StructField("seller_id", IntegerType()),
    StructField("star_sgg_id", IntegerType()),
    StructField("time_sgg_id", IntegerType()),
    StructField("discount", IntegerType()),
    StructField("discount_rate", IntegerType()),
    StructField("original_price", IntegerType()),
    StructField("price", IntegerType()),
    StructField("rating_average", FloatType()),
    StructField("review_count", IntegerType())
])

#Fact_Product
fact_product_schema = StructType([
    StructField("category_id", IntegerType()),
    StructField("brand_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("gift_sgg_id", IntegerType()),
    StructField("inventory_sgg_id", IntegerType()),
    StructField("url_sgg_id", StringType()),
    StructField("seller_id", IntegerType()),
    StructField("star_sgg_id", IntegerType()),
    StructField("time_sgg_id", IntegerType()),
    StructField("all_time_quantity_sold", IntegerType()),
    StructField("day_ago_created", IntegerType()),
    StructField("favourite_count", IntegerType()),
    StructField("list_price", IntegerType()),
    StructField("quantity_sold", IntegerType())
])
