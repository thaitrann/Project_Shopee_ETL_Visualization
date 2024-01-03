from setup import *
        
def create_tables(dwh_tables):
    for table in dwh_tables:
        path = "/datawarehouse/{}".format(table)
        if not fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)):
            fs.mkdirs(Path(path))
            print("Table created: {}!".format(table))
        else:
            print("Table already exists: {}!".format(table))
        
create_tables(dwh_tables)