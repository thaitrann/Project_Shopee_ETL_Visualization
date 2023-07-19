from setup import *

collections = ['collection_products_serp', 'collection_products_detail']
def Ingestion(collections):
    for collection in collections:
        uri = "mongodb://localhost:27017/products_tiki.{}".format(collection)

        #get the latest record in datalake 
        fs = FileSystem.get(URI("hdfs://localhost:19000"), Configuration())
        exists = fs.exists(Path('/datalake/{}'.format(collection)))
        location_datalake = 'hdfs://localhost:19000/datalake/{}'.format(collection)
        if exists:
            df_datalake = spark.read.parquet(location_datalake)
            max_date_df_datalake = df_datalake.select(F.max(F.col("completion_time"))).rdd.first()[0]
        else:
            max_date_df_datalake = datetime.datetime.fromtimestamp(0)

        #get the latest record in mongodb 
        mongo_df = spark.read.format("mongo").option("uri", uri).load()
        output_mongo_df = mongo_df.filter(mongo_df["completion_time"] > max_date_df_datalake)
        output_mongo_df = output_mongo_df.\
            withColumn("completion_year", date_format(col("completion_time"), "Y")).\
            withColumn("completion_month", date_format(col("completion_time"), "M")).\
            withColumn("completion_day", date_format(col("completion_time"), "d"))  
        print('--- Ingestion to table: {} ---'.format(collection))
        print('--- Number of lines to ingest: {} ---'.format(output_mongo_df.count()))

        #ingest to datalake
        output_mongo_df.write.partitionBy('completion_year', 'completion_month', 'completion_day')\
            .mode('append')\
            .parquet(location_datalake)

        print("--- Done Ingestion! ---")
        print(f"Execution time: {time.time() - start_time}")
        print("--------------------------------------")

Ingestion(collections)