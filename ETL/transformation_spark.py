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
# dim_category()

def dim_product():
    selected_col = read_parquet_collection_products_serp("product_id", "product_name")
    load_dl_to_dwh(dim_product_location, dim_product_df, dim_product_name, selected_col)
# dim_product()

def dim_configurable_product():
    default_values = array([lit("None"), lit("None"), lit("None"), lit(0), lit("None")])
    selected_col = read_parquet_collection_products_detail("product_id","seller_id","configurable_products")
    selected_col = selected_col.withColumn("configurable_products", when(size(selected_col["configurable_products"]) == 0, default_values).otherwise(col("configurable_products")))
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
    print(df_final.show())
    print(df_final.count())
    pd_df = df_exploded.toPandas()
    pd_df.to_excel("E:/test.xlsx")
dim_configurable_product()

def dim_inventory():
    sgg_id = 'inventory_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_detail("inventory_status", "inventory_type"), sgg_id)
    load_dl_to_dwh(dim_inventory_location, dim_inventory_df, dim_inventory_name, selected_col)
# dim_inventory()

def dim_seller():
    selected_col = read_parquet_collection_products_serp("seller_id", "seller_name")
    load_dl_to_dwh(dim_seller_location, dim_seller_df, dim_seller_name, selected_col)
# dim_seller()

def dim_star():
    pass

def dim_brand():
    selected_col = read_parquet_collection_products_serp("brand_id", "brand_name")
    load_dl_to_dwh(dim_brand_location, dim_brand_df, dim_brand_name, selected_col)
# dim_brand()

def dim_shipping():
    sgg_id = 'shipping_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_serp("shipping_code","shipping_text"), sgg_id)
    load_dl_to_dwh(dim_shipping_location, dim_shipping_df, dim_shipping_name, selected_col)
# dim_shipping()

def dim_gift():
    sgg_id = 'gift_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_detail("gift_item_title"), sgg_id)
    load_dl_to_dwh(dim_gift_location, dim_gift_df, dim_gift_name, selected_col)
# dim_gift()

def dim_url():
    sgg_id = 'url_sgg_id'
    selected_col = add_sgg_id_to_df(read_parquet_collection_products_detail("url"), sgg_id)
    load_dl_to_dwh(dim_url_location, dim_url_df, dim_url_name, selected_col)
# dim_url()

def dim_time():
    pass

def fact_sales():
    pass

def fact_product():
    pass

