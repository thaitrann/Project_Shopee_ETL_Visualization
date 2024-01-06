from setup import *

def delete_data_mongodb(database, collection):
    for collection_name in collections:
        collection = database[collection_name]
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} documents: {collection_name}.")
        print("Run time: ", time.time() - start_time)
        print("-----------------------------------------")
    
delete_data_mongodb(products_tiki, collections)
