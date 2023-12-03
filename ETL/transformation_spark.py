# transform from HDFS datalake to HDFS data warehouse
from setup import *
from schema import *

dl_collection_products_detail = spark.read.parquet("hdfs://localhost:19000/datalake/collection_products_detail")

selected_col = dl_collection_products_detail.dropDuplicates(["category_id", "category_name"])
print(selected_col.show())
print(selected_col.count())
# if dim_category_df.count() == 0:
#     dl_collection_products_detail.write.partitionBy('completion_year', 'completion_month', 'completion_day')\
#         .mode('append')\
#         .parquet(location_table_dwh)
# else:
#     # Lấy dữ liệu mới nhất từ bảng B
#     max_b = df_b.agg({"timestamp_column": "max"}).collect()[0][0]
    
#     # Lọc những dữ liệu mới hơn dữ liệu đã tồn tại trong bảng B
#     df_a_new_filtered = df_a_new.filter(df_a_new["timestamp_column"] > max_b)
    
#     # Kiểm tra xem có dữ liệu mới hay không
#     if df_a_new_filtered.count() == 0:
#         print("Không có dữ liệu mới nhất.")
#     else:
#         # Ghi dữ liệu mới vào bảng B và thêm partition bằng thời gian mới nhất
#         df_a_new_filtered.write.format("parquet") \
#             .mode("append") \
#             .partitionBy("timestamp_column") \
#             .save("hdfs://path_to_data_warehouse/table_b")