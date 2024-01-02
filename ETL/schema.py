from setup import *

dwh_tables = ["Dim_Category", "Dim_Product", "Dim_ConfigurableProduct","Dim_Inventory", "Dim_Seller", "Dim_Brand", "Dim_Shipping", \
    "Dim_Gift", "Dim_Url", "Dim_Time","Fact_Sales", "Fact_Product"]
        
def create_tables(dwh_tables):
    for table in dwh_tables:
        path = "/datawarehouse/{}".format(table)
        if not fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)):
            fs.mkdirs(Path(path))
            print("Table created: {}!".format(table))
        else:
            print("Table already exists: {}!".format(table))
            
def create_schema_location(table_name, schema):
    location_df = 'hdfs://localhost:19000/datawarehouse/{}'.format(table_name)
    dim_df = spark.read.schema(schema).parquet(location_df)
    return location_df, dim_df
        
create_tables(dwh_tables)

#schema
#Dim_Category
dim_category_schema = StructType([
    StructField("category_id", IntegerType(), nullable = False),
    StructField("category_name", StringType(), nullable = False)
])
dim_category_name = dwh_tables[0]
dim_category_location, dim_category_df = create_schema_location(dim_category_name, dim_category_schema)

#Dim_Product
dim_product_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False)
])
dim_product_name = dwh_tables[1]
dim_product_location, dim_product_df = create_schema_location(dim_product_name, dim_product_schema)

#Dim_ConfigurableProduct
dim_configurable_product_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("seller_id", IntegerType(), nullable=False),
    StructField("configurable_product_id", IntegerType(), nullable=False),
    StructField("configurable_product_name", StringType(), nullable=False),
    StructField("configurable_product_type", StringType(), nullable=False),
    StructField("configurable_product_price", DoubleType(), nullable=False),
    StructField("configurable_product_status", StringType(), nullable=False)
])
dim_configurable_product_name = dwh_tables[2]
dim_configurable_product_location, dim_configurable_product_df = create_schema_location(dim_configurable_product_name, dim_configurable_product_schema)

#Dim_Inventory
dim_inventory_schema = StructType([
    StructField("inventory_sgg_id", StringType(), nullable=False),
    StructField("inventory_status", StringType(), nullable=False),
    StructField("inventory_type", StringType(), nullable=False)
])
dim_inventory_name = dwh_tables[3]
dim_inventory_location, dim_inventory_df = create_schema_location(dim_inventory_name, dim_inventory_schema)

#Dim_Seller
dim_seller_schema = StructType([
    StructField("seller_id", IntegerType(), nullable=False),
    StructField("seller_name", StringType(), nullable=False),
    StructField("seller_level", StringType(), nullable=False)
])
dim_seller_name = dwh_tables[4]
dim_seller_location, dim_seller_df = create_schema_location(dim_seller_name, dim_seller_schema)

#Dim_Brand
dim_brand_schema = StructType([
    StructField("brand_id", IntegerType(), nullable=False),
    StructField("brand_name", StringType(), nullable=False)
])
dim_brand_name = dwh_tables[5]
dim_brand_location, dim_brand_df = create_schema_location(dim_brand_name, dim_brand_schema)

#Dim_Shipping
dim_shipping_schema = StructType([
    StructField("shipping_sgg_id", StringType(), nullable=False),
    StructField("shipping_code", StringType(), nullable=False),
    StructField("shipping_text", StringType(), nullable=False)
])
dim_shipping_name = dwh_tables[6]
dim_shipping_location, dim_shipping_df = create_schema_location(dim_shipping_name, dim_shipping_schema)

#Dim_Gift
dim_gift_schema = StructType([
    StructField("gift_sgg_id", StringType(), nullable=False),
    StructField("gift_item_title", StringType(), nullable=False)
])
dim_gift_name = dwh_tables[7]
dim_gift_location, dim_gift_df = create_schema_location(dim_gift_name, dim_gift_schema)

#Dim_Url
dim_url_schema = StructType([
    StructField("url_sgg_id", StringType(), nullable=False),
    StructField("url", StringType(), nullable=False)
])
dim_url_name = dwh_tables[8]
dim_url_location, dim_url_df = create_schema_location(dim_url_name, dim_url_schema)

#Dim_Time
dim_time_schema = StructType([
    StructField("time_id", StringType(), nullable=False),
    StructField("date", DateType(), nullable=False),
    StructField("day_of_week", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("quarter", IntegerType(), nullable=False),
    StructField("year", IntegerType(), nullable=False),
    StructField("hour", IntegerType(), nullable=False),
    StructField("minute", IntegerType(), nullable=False),
    StructField("second", IntegerType(), nullable=False)
])
dim_time_name = dwh_tables[9]
dim_time_location, dim_time_df = create_schema_location(dim_time_name, dim_time_schema)

#Fact_Sales
fact_sales_schema = StructType([
    StructField("category_id", IntegerType()),
    StructField("brand_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("gift_sgg_id", StringType()),
    StructField("inventory_sgg_id", StringType()),
    StructField("url_sgg_id", StringType()),
    StructField("seller_id", IntegerType()),
    StructField("star_sgg_id", StringType()),
    StructField("time_sgg_id", StringType()),
    StructField("discount", IntegerType()),
    StructField("discount_rate", IntegerType()),
    StructField("original_price", IntegerType()),
    StructField("price", IntegerType()),
    StructField("rating_average", FloatType()),
    StructField("review_count", IntegerType()),
    StructField("1_star_count", IntegerType()),
    StructField("1_star_percent", FloatType()),
    StructField("2_star_count", IntegerType()),
    StructField("2_star_percent", FloatType()),
    StructField("3_star_count", IntegerType()),
    StructField("3_star_percent", FloatType()),
    StructField("4_star_count", IntegerType()),
    StructField("4_star_percent", FloatType()),
    StructField("5_star_count", IntegerType()),
    StructField("5_star_percent", FloatType())
])
fact_sales_name = dwh_tables[10]
fact_sales_location, fact_sales_df = create_schema_location(fact_sales_name, fact_sales_schema)

#Fact_Product
fact_product_schema = StructType([
    StructField("category_id", IntegerType()),
    StructField("brand_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("gift_sgg_id", StringType()),
    StructField("inventory_sgg_id", StringType()),
    StructField("url_sgg_id", StringType()),
    StructField("seller_id", IntegerType()),
    StructField("star_sgg_id", StringType()),
    StructField("time_sgg_id", StringType()),
    StructField("all_time_quantity_sold", IntegerType()),
    StructField("day_ago_created", IntegerType()),
    StructField("favourite_count", IntegerType()),
    StructField("list_price", IntegerType()),
    StructField("quantity_sold", IntegerType())
])
fact_product_name = dwh_tables[11]
fact_product_location, fact_product_df = create_schema_location(fact_product_name, fact_product_schema)