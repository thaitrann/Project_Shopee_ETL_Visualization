# transform from HDFS datalake to HDFS data warehouse
from setup import *
from schema import *

dl_collection_products_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")

selected_col = dl_collection_products_detail.select("category_id", "category_name").distinct()

if dim_category_df.count() == 0:
    new_data = selected_col
    print("Bảng trống! Đang đổ dữ liệu mới vào")
    new_data.write.mode('append').parquet(location_table_dwh)
else:
    new_data = selected_col.subtract(dim_category_df)

    if new_data.count() == 0:
        print("Không có dữ liệu mới nhất.")
    else:
        print("Đang cập nhật dữ liệu mới vào bảng")
        new_data.write.mode('append').parquet(location_table_dwh)

test = spark.read.parquet(location_table_dwh)
print(test.show(test.count(), truncate=False))
print(test.count())