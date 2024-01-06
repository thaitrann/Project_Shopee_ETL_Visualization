from setup import *

def delete_data_datawarehouse(tables):
    for table in tables:
        path = "/datawarehouse/{}".format(table)
        status = fs.listStatus(Path(path))
        for fileStatus in status:
            sub_file_path = fileStatus.getPath()
            if sub_file_path:
                fs.delete(sub_file_path)
        print("Deleted Data Table: {}!".format(table))
            
    print("Run time: ", time.time() - start_time)
    print("-----------------------------------------")

def delete_data_datalake(collections):
    for collection in collections:
        path = "/datalake/{}".format(collection)
        if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)):
            fs.delete(Path(path))
            print("Table Deleted: {}!".format(collection))
        else:
            print("Table Not Exists: {}!".format(collection))
            
    print("Run time: ", time.time() - start_time)
    print("-----------------------------------------")

 
delete_data_datalake(collections)
delete_data_datawarehouse(dwh_tables)




    
