# transform from HDFS datalake to HDFS data warehouse
from setup import *
from schema import *

#read source data lake
def read_parquet_collection_products_serp(*selected_columns):
    df = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")
    selected_col = df.select(*selected_columns).distinct()
    return selected_col

def read_parquet_collection_products_detail(*selected_columns):
    df = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")
    selected_col = df.select(*selected_columns).distinct()
    return selected_col

def load_dl_to_dwh(location, df, table_name, selected_col):
    if df.count() == 0:
        new_data = selected_col
        print("Empty table! Loading new data")
        new_data.write.mode('append').parquet(location)
    else:
        new_data = selected_col.subtract(df)
        if new_data.count() == 0:
            print("No new data!")
        else:
            print("Updating new data into the table!")
            new_data.write.mode('append').parquet(location)
            
    table = spark.read.parquet(location)
    print("Done load table: {}!".format(table_name))
    print(table.show(table.count(), truncate=False))
    print("Number of rows in the table: {}".format(table.count()))
    print("Run time: ", time.time() - start_time)
    print("-----------------------------------------")

def add_sgg_id_to_df(df, sgg_id):
    counter = 0
    counter += 1
    result_df = df.withColumn(sgg_id, format_string("%s%%06d" % sgg_id, (monotonically_increasing_id() + 1)))
    result_df = result_df.select(sgg_id, *df.columns)
    return result_df

#dag
def dim_category():
    selected_col = read_parquet_collection_products_detail("category_id", "category_name")
    load_dl_to_dwh(dim_category_location, dim_category_df, dim_category_name, selected_col)

def dim_product():
    selected_col = read_parquet_collection_products_serp("product_id", "product_name")
    load_dl_to_dwh(dim_product_location, dim_product_df, dim_product_name, selected_col) 

def dim_configurable_product():
    selected_col = read_parquet_collection_products_detail("product_id","seller_id","configurable_products")
    df_exploded = selected_col.withColumn("configurable_products_detail", explode(selected_col["configurable_products"]))
    df_final = df_exploded.select(
    "product_id",
    "seller_id",
    df_exploded["configurable_products_detail"][0].alias("configurable_products_id"),
    df_exploded["configurable_products_detail"][1].alias("configurable_products_name"),
    df_exploded["configurable_products_detail"][2].alias("configurable_products_type"),
    df_exploded["configurable_products_detail"][3].alias("configurable_products_price"),
    df_exploded["configurable_products_detail"][4].alias("configurable_products_status")
)
    load_dl_to_dwh(dim_configurable_product_location, dim_configurable_product_df, dim_configurable_product_name, df_final)
    # print(df_final.show())
    # print(df_final.count())
    # pd_df = df_final.toPandas()
    # pd_df.to_excel("E:/test.xlsx")

def dim_inventory():
    sgg_id = 'inventory_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_detail("inventory_status", "inventory_type"), sgg_id)
    load_dl_to_dwh(dim_inventory_location, dim_inventory_df, dim_inventory_name, selected_col)

def dim_seller():
    selected_col = read_parquet_collection_products_detail("seller_id", "seller_name", "seller_level")
    load_dl_to_dwh(dim_seller_location, dim_seller_df, dim_seller_name, selected_col)

def dim_brand():
    selected_col = read_parquet_collection_products_serp("brand_id", "brand_name")
    load_dl_to_dwh(dim_brand_location, dim_brand_df, dim_brand_name, selected_col)

def dim_shipping():
    sgg_id = 'shipping_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_serp("shipping_code","shipping_text"), sgg_id)
    load_dl_to_dwh(dim_shipping_location, dim_shipping_df, dim_shipping_name, selected_col)

def dim_gift():
    sgg_id = 'gift_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_detail("gift_item_title"), sgg_id)
    load_dl_to_dwh(dim_gift_location, dim_gift_df, dim_gift_name, selected_col)

def dim_url():
    sgg_id = 'url_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_detail("url"), sgg_id)
    load_dl_to_dwh(dim_url_location, dim_url_df, dim_url_name, selected_col)

def dim_time_collect_id():
    time_df = read_parquet_collection_products_serp("completion_time")

    time_df = time_df.withColumn("date", col("completion_time").cast(DateType())) \
    .withColumn("day_of_week", dayofweek(col("completion_time"))) \
    .withColumn("month", month(col("completion_time"))) \
    .withColumn("quarter", quarter(col("completion_time"))) \
    .withColumn("year", year(col("completion_time"))) \
    .withColumn("hour", hour(col("completion_time"))) \
    .withColumn("minute", minute(col("completion_time"))) \
    .withColumn("second", second(col("completion_time"))) \
    .withColumn("time_collect_id", concat(date_format(col("completion_time"), "yyyyMMddHHmmss")))
    
    selected_col = time_df.select("time_collect_id", "date", "day_of_week", "month", "quarter", "year", "hour", "minute", "second")
    load_dl_to_dwh(dim_time_collect_id_location, dim_time_collect_id_df, dim_time_collect_id_name, selected_col)  
    
def dim_time_collect_detail():
    time_df = read_parquet_collection_products_detail("completion_time")

    time_df = time_df.withColumn("date", col("completion_time").cast(DateType())) \
    .withColumn("day_of_week", dayofweek(col("completion_time"))) \
    .withColumn("month", month(col("completion_time"))) \
    .withColumn("quarter", quarter(col("completion_time"))) \
    .withColumn("year", year(col("completion_time"))) \
    .withColumn("hour", hour(col("completion_time"))) \
    .withColumn("minute", minute(col("completion_time"))) \
    .withColumn("second", second(col("completion_time"))) \
    .withColumn("time_detail_id", concat(date_format(col("completion_time"), "yyyyMMddHHmmss")))
    
    selected_col = time_df.select("time_detail_id", "date", "day_of_week", "month", "quarter", "year", "hour", "minute", "second")
    load_dl_to_dwh(dim_time_collect_detail_location, dim_time_collect_detail_df, dim_time_collect_detail_name, selected_col)  

def fact_sales():
    df_serp = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")
    df_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")
    dim_inventory = spark.read.parquet("hdfs://localhost:19000/datawarehouse/Dim_Inventory")
    dim_shipping = spark.read.parquet("hdfs://localhost:19000/datawarehouse/Dim_Shipping")
    dim_gift = spark.read.parquet("hdfs://localhost:19000/datawarehouse/Dim_Gift")
    dim_url = spark.read.parquet("hdfs://localhost:19000/datawarehouse/Dim_Url")
    
    join_condition_inventory = (df_detail["inventory_status"] == dim_inventory["inventory_status"]) & (df_detail["inventory_type"] == dim_inventory["inventory_type"])
    join_condition_gift = (df_detail["gift_item_title"] == dim_gift["gift_item_title"])
    join_condition_url = (df_detail["url"] == dim_url["url"])
    join_condition_shipping = (df_serp["shipping_code"] == dim_shipping["shipping_code"]) & (df_serp["shipping_text"] == dim_shipping["shipping_text"])
    
    joined_df = df_detail.join(dim_inventory, join_condition_inventory, "inner")\
                        .join(dim_gift, join_condition_gift, "inner")\
                        .join(dim_url, join_condition_url, "inner")

    joined_df_serp = df_serp.join(dim_shipping, join_condition_shipping, "inner")
    
    test2 = joined_df_serp.select(df_serp["product_id"], df_serp["seller_id"], dim_shipping["shipping_code"], dim_shipping["shipping_text"], dim_shipping["shipping_sgg_id"])
    
    test = joined_df.select(df_detail["product_id"], df_detail["seller_id"], dim_inventory["inventory_sgg_id"], \
        df_detail["inventory_status"], df_detail["inventory_type"], dim_gift["gift_sgg_id"], dim_gift["gift_item_title"],\
        dim_url["url_sgg_id"], df_detail["url"])
    
    print(test2.show(test2.count(), truncate=False))
    print(test2.count())

def fact_product():
    pass

dim_category()
dim_product()
dim_configurable_product()
dim_inventory()
dim_seller()
dim_brand()
dim_shipping()
dim_gift()
dim_url()
dim_time_collect_id()
dim_time_collect_detail()
# fact_sales()

#fix data time đang sai

