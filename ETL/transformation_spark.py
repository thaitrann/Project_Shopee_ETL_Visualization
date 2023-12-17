# transform from HDFS datalake to HDFS data warehouse
from setup import *
from schema import *

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

def dim_category():
    dl_collection_products_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")
    selected_col = dl_collection_products_detail.select("category_id", "category_name").distinct()
    load_dl_to_dwh(dim_category_location, dim_category_df, dim_category_name, selected_col)
# dim_category()

def dim_product():
    dl_collection_products_serp = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")
    selected_col = dl_collection_products_serp.select("product_id", "product_name").distinct()
    load_dl_to_dwh(dim_product_location, dim_product_df, dim_product_name, selected_col)
# dim_product()

def dim_configurable_product():
    pass

def dim_inventory():
    sgg_id = 'inventory_sgg_id'
    dl_collection_products_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")
    selected_col = dl_collection_products_detail.select("inventory_status", "inventory_type").distinct()
    selected_col = add_sgg_id_to_df(selected_col, sgg_id)
    load_dl_to_dwh(dim_inventory_location, dim_inventory_df, dim_inventory_name, selected_col)
# dim_inventory()

def dim_seller():
    dl_collection_products_serp = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")
    selected_col = dl_collection_products_serp.select("seller_id", "seller_name").distinct()
    load_dl_to_dwh(dim_seller_location, dim_seller_df, dim_seller_name, selected_col)
# dim_seller()

def dim_star():
    pass

def dim_brand():
    dl_collection_products_serp = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")
    selected_col = dl_collection_products_serp.select("brand_id", "brand_name").distinct()
    load_dl_to_dwh(dim_brand_location, dim_brand_df, dim_brand_name, selected_col)
# dim_brand()

def dim_shipping():
    sgg_id = 'shipping_sgg_id'
    dl_collection_products_serp = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_serp")
    selected_col = dl_collection_products_serp.select("shipping_code","shipping_text").distinct()
    selected_col = add_sgg_id_to_df(selected_col, sgg_id)
    load_dl_to_dwh(dim_shipping_location, dim_shipping_df, dim_shipping_name, selected_col)
# dim_shipping()

def dim_gift():
    sgg_id = 'gift_sgg_id'
    dl_collection_products_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")
    selected_col = dl_collection_products_detail.select("gift_item_title").distinct()
    selected_col = add_sgg_id_to_df(selected_col, sgg_id)
    load_dl_to_dwh(dim_gift_location, dim_gift_df, dim_gift_name, selected_col)
dim_gift()

def dim_url():
    sgg_id = 'url_sgg_id'
    dl_collection_products_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")
    selected_col = dl_collection_products_detail.select("url").distinct()
    selected_col = add_sgg_id_to_df(selected_col, sgg_id)
    load_dl_to_dwh(dim_url_location, dim_url_df, dim_url_name, selected_col)
# dim_url()

def dim_time():
    pass

def fact_sales():
    pass

def fact_product():
    pass

